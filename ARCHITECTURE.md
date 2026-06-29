# Architecture & Design Deep-Dive

This document covers the design decisions behind the pipeline in depth: how data
changes shape at each stage, how the medallion layers work, how CDC events are
ordered and deduplicated, how deletes and snapshots are handled, how multiple
consumers share one stream, and how storage retention is managed.

For the high-level overview, setup, and component list, see the
[README](README.md).

## Data Format Journey

The same record changes representation many times between the generator and the
dashboard. Two transformations matter most: where it becomes **JSON** (Debezium)
and where it becomes **Parquet** (PySpark/Iceberg). Tracing one value —
`total_amount = 4799.20` — through every stage:

| # | Stage | Format | Example |
|---|-------|--------|---------|
| 1 | `generate.py` | Python objects | `total = 4799.20` (`float`) |
| 2 | Postgres table (heap) | Typed SQL row | `NUMERIC(12,2)` on disk |
| 3 | WAL | Binary log | `00 00 01 6A 3F 80 ...` (unreadable) |
| 4 | pgoutput plugin | Decoded binary message | row-level change, still binary |
| 5 | Debezium | **JSON envelope** | `{"payload":{"after":{...},"op":"c","source":{"lsn":...}}}` |
| 6 | Kafka topic | UTF-8 JSON bytes | byte array (Kafka is content-agnostic) |
| 7 | PySpark | DataFrame | `from_json` + `StructType` → typed columns |
| 8 | Iceberg bronze | **Parquet** (raw JSON inside) | metadata + dedup key as columns, full payload as JSON string |
| 9 | dbt silver/gold | Parquet (transformed) | cleaned, joined, aggregated |
| 10 | Superset | SQL result set | rows fetched via Thrift → rendered as charts |

Note: the WAL itself is binary — `pgoutput` decodes it to a structured message
*inside Postgres*, but the conversion to JSON only happens at the Debezium layer
via `JsonConverter`. (`decimal.handling.mode: string` keeps `total_amount` as a
string in the JSON to avoid floating-point precision loss in transit.)

## Data Layers

| Layer | Schema | Storage | Updated By |
|-------|--------|---------|------------|
| Bronze | `lakehouse.bronze` | Iceberg (MinIO, JDBC catalog) | PySpark streaming (continuous) |
| Staging | `staging` | Spark views (in-memory) | dbt (nightly) |
| Silver | `lakehouse.silver` | Iceberg (MinIO, JDBC catalog) | dbt (nightly) |
| Gold | `lakehouse.gold` | Iceberg (MinIO, JDBC catalog) | dbt (nightly) |

**Why layers at all?** Most source systems are **mutable** — an OLTP database
overwrites old values on every UPDATE. When an order moves from CREATED to
PAID, the CREATED state is gone forever in Postgres. CDC captures every change
before it disappears, but the captured events need to be organized. Raw events
go to bronze; cleaned, deduplicated rows go to silver; business-ready
aggregates go to gold. Each layer serves a different audience and a different
purpose.

**Bronze — raw, append-only, complete history.** Every CDC event lands here
as CDC metadata (`op`, `lsn`, `ts_ms`) + the dedup key (e.g. `order_id`) as
typed columns, plus the **entire Debezium payload as a raw JSON string**
(`raw_payload`). The same `order_id` appears multiple times — once for CREATED,
once for PAID, maybe once for CANCELLED. Nothing is updated, nothing is
deleted. This is the source of truth for the entire pipeline; every downstream
layer can be rebuilt from bronze. It exists because the mutable source database
does not preserve history — bronze does.

**Why store the raw payload instead of parsed columns?** If bronze pinned a
fixed column list, a new source column (say `discount_amount` added to `orders`)
would silently be dropped at ingest — it arrives in Kafka but the parser ignores
it. Six months later, when analytics finally needs that column, the historical
values are gone: Kafka's retention window has expired and the OLTP source only
keeps the *current* value, not the history. Storing the full payload as JSON
avoids this entirely. Whatever Debezium emits is captured verbatim, so any
column — present or future — is already in bronze. To start using a new column
you just add one `get_json_object(raw_payload, '$.new_col')` line in staging;
bronze never changes and the history is already there. The trade-off is a JSON
parse cost on read, which is acceptable because bronze is the *capture-and-store*
layer — interpretation belongs upstream in staging/silver. The dedup key is
kept as a separate typed column (not parsed from JSON each time) so
`PARTITION BY` stays clean and fast.

**The ingest must not re-parse with a fixed schema either.** A subtle trap:
even with a JSON `raw_payload` column, if the streaming job parsed the Kafka
message with a fixed `StructType` first (`from_json`) and *then* re-serialized
it, any field missing from that `StructType` — including a newly added source
column — would be dropped during the parse, before it ever reached
`raw_payload`. So the streaming job deliberately does **not** parse the message
into a struct. It pulls only the fields it needs (`op`, `lsn`, `ts_ms`, the
dedup key) via `get_json_object` JSON paths and stores `payload.after` /
`payload.before` as the raw string. Nothing is schema-bound at ingest, so a new
column genuinely flows through to bronze untouched.

*Without bronze, today nothing breaks* — silver and gold tables live in their
own Parquet files, reports keep working. But tomorrow the damage starts:

- *Nightly pipeline breaks.* `dbt run` fails because staging views read from
  `{{ source('bronze', 'orders') }}`. No bronze → no staging → no silver → no
  gold. Reports freeze at the last successful run.
- *Bug fixes become impossible.* You discover `paid_amount` has been
  miscalculated for 3 months. You fix the dbt model and run
  `dbt run --full-refresh` — but full refresh rebuilds silver from bronze. No
  bronze, no fix. You are stuck with 3 months of wrong revenue numbers.
- *New columns cannot be back-filled.* Business asks "show me cancellations by
  city." You need to join orders with user city — that enrichment comes from
  bronze `users` events. Without bronze you can only start from today; the last
  6 months of orders have no city.
- *New metrics cannot be computed retroactively.* "What was our average
  order-to-payment time last quarter?" requires both the CREATED and PAID
  events for the same order — bronze has both as separate rows. Silver only
  keeps the final state (PAID); the CREATED timestamp is there but the
  intermediate event history is lost. Gold has daily aggregates — individual
  order timing is gone entirely.
- *Audit and compliance gaps.* "Prove that order #12345 was CREATED before it
  was CANCELLED." Bronze has both events with WAL LSN timestamps. Silver has
  only the latest state. If a regulator or finance team asks for the sequence
  of state changes, only bronze can answer.

Bronze is cheap insurance — pennies per GB/month on object storage — against
all of the above. You rarely read old bronze data day-to-day, but when you
need it, nothing else can substitute.

**Staging — views that deduplicate.** Lightweight SQL views (not physical
tables) that read bronze, apply `ROW_NUMBER() OVER (PARTITION BY order_id
ORDER BY lsn DESC)`, and expose only the latest version of each row. They
vanish on Spark restart and are re-created by `dbt run` — by design, since
they cost nothing to rebuild.

**Silver — cleaned, enriched, business-entity tables.** Materialized Iceberg
tables that join staged data (orders + users, order\_items + products), apply
business rules, and add derived columns (`paid_amount`, `is_cancelled`). One
row per business entity. This is where analysts start querying.

**Gold — aggregated, report-ready tables.** Pre-computed metrics:
`mart_daily_revenue` (daily totals), `mart_sales_by_category` (category
breakdown). Superset dashboards read from gold. These tables answer recurring
business questions without requiring analysts to write complex joins.

## Handling Updates: CDC Event Ordering

In a CDC pipeline a single row changes over time. An order moves
`CREATED → PAID` (or `CANCELLED`), so Debezium emits **several events for the
same `order_id`**. Bronze is append-only by design, so it stores every version
of the row — the silver layer is responsible for collapsing them down to the
latest state. The hard part is deciding which version *is* the latest.

A naive approach orders by `created_at`:

```sql
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC)
```

This is **wrong** here. `created_at` is set once at INSERT (`DEFAULT now()`)
and is never touched on UPDATE — correct semantics for a *creation* timestamp.
So the `CREATED` and `PAID` rows of the same order carry an identical
`created_at`, the ordering becomes non-deterministic, and `ROW_NUMBER` can keep
the stale `CREATED` row. Revenue ends up undercounted.

The fix is to order by the database's own source of truth for change order: the
Postgres **WAL LSN** (Log Sequence Number). Every committed change gets a
unique, monotonically increasing LSN, exposed by Debezium in
`payload.source.lsn`. The streaming job captures it (plus `source.ts_ms` as a
tiebreaker) into bronze, and staging dedups on it:

```sql
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY lsn DESC, ts_ms DESC)
```

| order_id | op | status  | lsn      | kept |
|----------|----|---------|----------|------|
| 5        | c  | CREATED | 24023000 |      |
| 5        | u  | PAID    | 24023128 | ✓    |

This is the canonical way to order CDC events. It keeps the source schema
untouched — no need to add an `updated_at` column to the OLTP database, which
you often cannot modify in production anyway.

### CDC Operation Handling (snapshot, create, update, delete)

Each Debezium event carries an `op` code: `r` (read / initial snapshot of rows
that already existed when CDC started), `c` (insert), `u` (update), `d`
(delete). All four are handled deliberately:

- **Snapshot (`op = 'r'`) rows are kept.** When the connector first starts it
  reads every existing row as `op = 'r'`. These are real records that predate
  the pipeline — dropping them would silently lose all data that existed before
  CDC was switched on. They flow through to silver like any create.
- **`CREATED` orders are kept.** `CREATED` is a valid lifecycle state, not
  noise — it powers analyses like unpaid-cart / abandoned-order reporting. The
  core model keeps every status (`CREATED`, `PAID`, `CANCELLED`) rather than
  filtering to terminal states only.
- **Deletes are handled as soft deletes (deliberate choice).** When a row is
  deleted in Postgres, Debezium emits `op = 'd'` with the row's values in
  `payload.before` (and `payload.after` null). The streaming job reads both
  `before` and `after` and coalesces them, so the delete is captured with its
  key intact. This is applied uniformly to **all CDC tables** (orders,
  order_items, users, products) — not just orders — so the delete policy is
  consistent across the warehouse. Downstream, instead of physically removing
  the row, every staging model sets `is_deleted = true` and keeps it. This is
  intentional: the lakehouse preserves history for audit and analytics even
  after the source row is gone. Marts then exclude `is_deleted = true` rows
  from revenue and sales metrics, so deleted records stay queryable without
  polluting business numbers. (`tombstones.on.delete` is `false` on the
  connector, so a delete is a single event with no trailing null tombstone.)
  The generator periodically deletes an old cancelled order so these `op = 'd'`
  events actually flow through the pipeline.

### Streaming Referential Consistency

`orders` and `order_items` are written to bronze as **independent streams**
with separate microbatches and checkpoints. At any instant the tail of one
stream can lead the other, so the newest order lines may briefly reference an
order that has not landed yet. This is normal eventual consistency, not a bug —
so the `order_id` referential-integrity tests are configured to **warn** on the
expected tail lag (`warn_if: ">0"`) and only **fail** on structural breakage
(`error_if: ">500"`), instead of demanding perfect consistency on a moving
target.

**A subtler variant: cross-table snapshot skew.** dbt reads its source tables
at slightly different moments within a single run — `orders` might be read at
02:00:00 and `order_items` at 02:00:30. Iceberg's **snapshot isolation**
guarantees each table is internally consistent (dbt sees one frozen snapshot
per table, even if PySpark keeps writing during the run), but the two
snapshots are taken 30 seconds apart. So an `order_item` read at 02:00:30 may
reference an `order_id` that wasn't yet in the `orders` snapshot from 02:00:00.
The same `warn_if`/`error_if` thresholds absorb this, and the orphan resolves
on the next run once the parent order is in bronze.

*Production solution (not yet implemented):* read **all** source tables at one
fixed cutoff using Iceberg time travel — `SELECT ... FROM orders TIMESTAMP AS OF
'{{ cutoff }}'` with the same `cutoff` injected into every staging model at the
start of the dbt run (e.g. via a dbt var set to the run's start time). This
gives a single consistent cross-table cut: referential integrity is guaranteed,
the run is reproducible (same cutoff → same result), and it's auditable (every
report maps to a known point in time). The trade-offs are minor for a nightly
batch: data after the cutoff waits for the next run, every model must thread the
cutoff parameter, and the snapshot must still exist (retention must outlast the
cutoff). We chose to document this rather than implement it for now, since the
threshold-based approach is sufficient at the current scale and the
point-in-time read adds parameter-threading complexity across the model graph.

## Multiple Consumers: The Stock Monitoring Service

The same CDC stream can feed more than one consumer. The analytics pipeline
(PySpark → bronze → dbt) is one consumer; the **stock monitoring service** is a
second, completely independent one. It reads the `ecom.public.inventory` topic —
which Debezium already produces — and raises a low-stock alert when a product
drops below a threshold.

**Stock control is the application's job, not CDC's.** When a customer places an
order, the backend (OLTP) checks stock, decrements it, and rejects the order if
inventory is insufficient — all inside a single transaction, in milliseconds.
By the time Debezium sees the `stock_qty: 50 → 47` change in the WAL, the
decision is already made and the stock is already reduced. CDC **observes the
result**; it does not make the decision.

So what is inventory data good for on the CDC side?

- **Alerting / monitoring** — not stock *management*, stock *observation*. Notify
  the purchasing team when a product is running low so they can reorder from the
  supplier. The application won't do this — its job is taking orders, not supply
  planning.
- **Analytics** — burn-rate analysis: how fast does a product sell, at what
  hours does it accelerate, when will it run out? This history does not exist in
  OLTP (which only holds the *current* stock); it exists in the bronze event
  stream.
- **Synchronization** — push inventory changes to other systems: marketplace
  integrations (selling on one platform should update stock on another),
  warehouse management, supplier portals. Rather than each system connecting to
  the OLTP database separately, they all read from the Kafka topic.

**What it demonstrates architecturally.** Adding this service required **zero
changes** to Postgres, Debezium, Kafka, or PySpark. The `inventory` table was
already in Debezium's `table.include.list` with `REPLICA IDENTITY FULL`, so the
topic was already flowing — just unconsumed. The new service simply attaches a
**new consumer group** (`stock-monitor-service`) to that topic. Kafka gives each
consumer group an independent copy of the stream with its own offsets, so the
stock monitor reads at its own pace without affecting the analytics pipeline.
This is Kafka's fan-out capability in action — the concrete payoff of putting a
log between the source and its consumers.

The example implementation (`stock-monitor/stock_monitor.py`) logs alerts to
stdout; in production the alert path would call a Slack webhook, email, or
PagerDuty.

## Data Retention & Storage Management

Data flows through several storage layers, each with its own retention
characteristics. No business data is ever lost — retention policies only
reclaim temporary or superseded storage, not source-of-truth records.

**Kafka (7-day replay window).** Topic retention defaults to
`retention.ms=604800000` (7 days). After 7 days, consumed messages are deleted
from the broker. This is safe because every event has already been written to
Iceberg bronze by PySpark. Kafka is a transit buffer, not long-term storage.
If PySpark needs to reprocess, it replays from Kafka within the 7-day window;
for anything older, bronze is the authoritative source.

**Iceberg snapshot expiration.** Every `dbt run` creates a new table snapshot —
a pointer to the set of Parquet files that represent the table at that moment.
Over time, snapshots accumulate. `expire_snapshots` removes old snapshots and
deletes Parquet files that are **no longer referenced by any remaining
snapshot**. Critically, the **current snapshot and its data are never touched**.
You lose the ability to time-travel to an expired point in time, but all
current rows remain intact. Think of it as clearing version history in a
document — the document itself stays, only the undo stack shrinks.

**Bronze is never deleted.** Bronze tables are append-only and live on cheap
object storage (MinIO / S3). They are the source of truth for the entire
pipeline. Silver and gold are derived — they can always be rebuilt from bronze
with `dbt run --full-refresh`. Deleting bronze would make reprocessing,
bug-fixing, and adding new columns to historical data impossible. The storage
cost of retaining bronze indefinitely (pennies per GB/month on S3) is
negligible compared to the cost of losing the ability to reprocess.

| Layer | What gets cleaned | What stays | Risk of deletion |
|-------|-------------------|------------|------------------|
| Kafka | Messages older than retention window | — | None — already in bronze |
| Iceberg snapshots | Old metadata + orphaned Parquet files | Current table state | Lose time travel only |
| Bronze | **Never** | All CDC events, all time | — |
| Silver / Gold | Rebuilt on every `dbt run` | Current transformed state | Rebuilt from bronze |
