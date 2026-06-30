# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A self-hosted, real-time e-commerce lakehouse demonstrating an end-to-end CDC pipeline.
Everything runs via Docker Compose on a single machine. There is no application code
to "run" outside containers — work happens by editing config/SQL/Python and rebuilding
the relevant service.

The deep design rationale lives in `ARCHITECTURE.md` (CDC ordering, medallion layers,
soft-delete handling, raw-payload bronze, retention). Read it before changing the
streaming job, the dedup logic, or the layer boundaries — many "obvious simplifications"
are deliberate trade-offs documented there. `RUNBOOK.md` has the operational command
cheat sheet; `TROUBLESHOOTING.md` has failure-mode fixes.

## Data flow (the big picture)

```
generator → Postgres (WAL) → Debezium → Kafka → PySpark → Iceberg bronze (MinIO)
                                          └→ stock-monitor (independent consumer)
bronze → dbt (staging views → silver → gold) → Spark Thrift → Superset / MCP server
```

- **One source of truth is bronze.** PySpark (`pyspark/orders_stream.py`) writes every
  CDC event append-only into `lakehouse.bronze.*` Iceberg tables. Silver and gold are
  always rebuildable from bronze via `dbt run --full-refresh`.
- **Two write paths into the same Iceberg catalog run concurrently:** the streaming job
  writes bronze continuously while dbt writes silver/gold nightly. This is why the
  Iceberg catalog is **JDBC-over-Postgres** (`iceberg-db`), not the Hadoop catalog —
  atomic commits are required for safe concurrent writers on MinIO. Do not switch to a
  file-based catalog.
- **Spark runs as two containers from the same `./pyspark` image:** `pyspark` (the
  streaming `spark-submit` job) and `spark-thrift` (a long-lived Thrift Server on port
  10000 that dbt, Superset, and the MCP server query over HiveServer2).

## Conventions that are load-bearing

- **Bronze stores the whole Debezium payload as a raw JSON string** (`raw_payload`),
  plus only CDC metadata (`op`, `lsn`, `ts_ms`) and the dedup PK as typed columns. The
  streaming job deliberately **never parses the Kafka value into a `StructType`** — it
  pulls fields with `get_json_object` JSON paths so a newly added source column flows
  through untouched. Adding a column to a model = one `get_json_object(...)` line in the
  `stg_*` model; the bronze schema never changes. Don't "improve" this by introducing a
  fixed schema at ingest.
- **CDC dedup orders by WAL LSN, never by `created_at`.** Staging models use
  `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY lsn DESC, ts_ms DESC)`. `created_at` is
  set once at INSERT and is identical across an order's CREATED/PAID rows, so ordering by
  it silently keeps stale rows.
- **Deletes are soft deletes, applied uniformly to all tables.** `op = 'd'` events are
  kept (coalescing `before`/`after` for the key); staging sets `is_deleted = true` and
  marts exclude those rows from business metrics. `tombstones.on.delete` is `false`.
- **Referential-integrity tests warn, not fail, on tail lag.** `orders` and
  `order_items` are independent streams, so brief orphan rows are expected eventual
  consistency. Relationship tests use `warn_if: ">0"` / `error_if: ">500"` — keep this
  pattern rather than demanding perfect consistency.

## dbt layout

- `dbt/models/staging/` → **views** in the default Spark catalog (`stg_*`, dedup + JSON
  extraction). Cheap to rebuild; vanish on Thrift restart by design.
- `dbt/models/core/` → **silver** Iceberg tables (`lakehouse.silver`, `core_*`).
- `dbt/models/mart/` → **gold** Iceberg tables (`lakehouse.gold`, `mart_*`).
- `dbt/macros/generate_schema_name.sql` overrides dbt's default so a custom `+schema:`
  like `lakehouse.silver` is used **verbatim** (producing correct three-part names)
  instead of being prefixed. Don't remove it or the Iceberg table names break.
- `dbt_project.yml`'s `on-run-start` creates the `lakehouse.silver` / `lakehouse.gold`
  namespaces (the JDBC catalog won't auto-create them).
- dbt connects over Thrift (`profiles.yml`, `host: spark-thrift`), so it only works
  from inside a container on the compose network (e.g. `ecom-airflow-scheduler`).

## Common commands

All run against containers (`ecom-*`). On Windows PowerShell use `curl.exe`, not `curl`.

```bash
# Lifecycle
docker compose up -d --build        # build + start everything
docker compose start                # resume after stop (no rebuild)
docker compose down -v && docker compose up -d --build   # FULL RESET (wipes volumes)
docker compose build --no-cache pyspark                  # rebuild after stale code

# Freeze data generation but keep the query/AI layer live (for stable demos)
docker compose stop generator pyspark stock-monitor connect

# dbt (run from the airflow scheduler, which has dbt + the project mounted)
docker exec ecom-airflow-scheduler dbt run  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
docker exec ecom-airflow-scheduler dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
docker exec ecom-airflow-scheduler dbt run --select stg_orders --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
# healthy dbt run/test ends with: PASS=8 WARN=0 ERROR=0 SKIP=0 TOTAL=8

# Query the lakehouse
docker exec -it ecom-spark-thrift /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000"
docker exec -it ecom-mcp-server python -c "import server; print(server.list_tables())"

# Health checks
curl.exe http://localhost:8083/connectors/ecommerce-connector/status   # Debezium
docker logs ecom-pyspark --tail 20                                     # bronze writes
```

After any reset, wait ~2-3 min for PySpark to populate bronze **before** running dbt.
There are no unit tests; dbt tests are the test suite.

## Gotchas

- **Postgres/Kafka/Spark checkpoints are coupled state.** If a schema or pipeline change
  makes them disagree, the fix is almost always a full `down -v` reset, not a partial
  restart — checkpoints in `s3a://lakehouse/checkpoints/*` pin Kafka offsets.
- **Shell scripts and Dockerfiles must stay LF** (`.gitattributes` enforces it). CRLF in
  `debezium/register-connector.sh` will break the connector-init container on Linux.
- **There are four separate Postgres instances — don't conflate them:** `postgres` (the
  source OLTP database, exposed on host port 5433) and three internal-only ones,
  `iceberg-db` (Iceberg JDBC catalog), `airflow-db` (Airflow metadata), and `superset-db`
  (Superset metadata).
- **MCP server stdout is reserved for the protocol.** `mcp-server/server.py` silences
  logging on purpose; any stray print to stdout breaks the stdio MCP transport.
- Code comments and runtime messages are English; keep that when editing.
