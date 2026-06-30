# Troubleshooting & Operational Notes

Real problems hit while building and running this pipeline, with their root
causes and fixes. Kept in the repo so the same issues don't cost time twice.

---

## CDC / Debezium

### Trailing slash in connector registration URL
**Symptom:** Debezium connector registration failed / behaved unexpectedly.
**Cause:** A trailing slash on the Kafka Connect REST endpoint
(`http://connect:8083/connectors/`) is not accepted the same way as the
slashless form.
**Fix:** Register against `http://connect:8083/connectors` (no trailing slash).

### `register-connector.sh` — `syntax error: unexpected end of file`
**Symptom:** `connector-init` exits with
`/register-connector.sh: line 14: syntax error: unexpected end of file (expecting "do")`.
**Cause:** The shell script was checked out on Windows with CRLF (`\r\n`) line
endings. The `\r` characters break the `until ... do ... done` loop inside the
Linux container.
**Fix:** Added `.gitattributes` forcing LF for shell scripts:
```
*.sh text eol=lf
Dockerfile text eol=lf
```
If a file is already CRLF locally, normalize it once:
```bash
# macOS / Linux
sed -i '' 's/\r$//' debezium/register-connector.sh
# Windows PowerShell
(Get-Content file.sh -Raw) -replace "`r`n","`n" | Set-Content file.sh -NoNewline
```

### `connector-init` reports success but no connector is registered
**Symptom:** `connector-init` logs `#!/bin/bash: not found` near the top, then
prints `Connector kaydedildi` ("connector registered") and exits 0 — but
`curl http://localhost:8083/connectors` returns `[]`, the `ecom.public.*` topics
never appear (except `inventory`), and PySpark crashes with
`UnknownTopicOrPartitionException: This server does not host this topic-partition`.
**Cause:** Two compounding bugs. (1) The script was saved with a UTF-8 **BOM**,
so the first line became `<BOM>#!/bin/bash` and the kernel could not read the
shebang. (2) The readiness loop only waited for the Connect REST port to
*accept a connection*, not to be *ready* — on a slow host (e.g. Apple Silicon)
Connect answers `404` while still starting, the loop exits, the POST hits that
`404`, and the script printed "registered" regardless because it never checked
the HTTP status. Registration silently failed.
**Fix:** Strip the BOM, and make the script wait for HTTP `200` from
`/connectors` before POSTing, then verify the POST returned `201`/`200`/`409`
(and `exit 1` otherwise) so a failure is loud, not silent. To recover a running
stack without a full reset, re-run the init container once Connect is up:
```bash
docker compose up -d connector-init
curl http://localhost:8083/connectors      # should now show ["ecommerce-connector"]
docker compose restart pyspark             # restart so it picks up the new topics
```

### `connector-init` returns 409 on restart
**Symptom:** `{"error_code":409,"message":"Connector ecommerce-connector already exists"}`.
**Cause:** The connector was already registered in a previous run (config
persists in Kafka Connect).
**Fix:** Not an error — expected on restart. The connector keeps running with
its existing config. Safe to ignore.

---

## Iceberg / Catalog

### Hadoop catalog corruption under concurrent writers
**Symptom:** Risk of corrupted commits when multiple writers touch the same
Iceberg table on MinIO.
**Cause:** The Hadoop catalog commits by renaming files on S3/MinIO, which is
not atomic and has no locking.
**Fix:** Migrated to a JDBC catalog backed by Postgres (`iceberg-db`). Commits
become atomic Postgres transactions.

### Silver/gold vanish after Thrift restart
**Symptom:** Silver and gold tables disappear when the Thrift server restarts.
**Cause:** They lived in the default in-memory catalog.
**Fix:** Write silver/gold to the Iceberg JDBC catalog with explicit
`+schema: lakehouse.silver` / `lakehouse.gold` and a `generate_schema_name`
macro override (dbt-spark forbids the `database` field, so the namespace is
embedded in the schema).

---

## Spark / Thrift

### Thrift server segfault (SIGSEGV)
**Symptom:** Spark Thrift server crashes during Iceberg scan planning.
**Cause:** A JIT compiler bug in Temurin 11 (SIGSEGV in
`PhaseLive::compute` when C2 optimizes certain code paths).
**Fix:** Start the Thrift server with
`--driver-memory 2g --driver-java-options -XX:TieredStopAtLevel=1`
(disables the C2 JIT tier).

---

## dbt

### `created_at` deduplication picks the wrong version
**Symptom:** PAID orders silently dropped, revenue undercounted.
**Cause:** `created_at` does not change between CREATED and PAID events, so
ordering by it produces non-deterministic ties.
**Fix:** Deduplicate with `ROW_NUMBER() OVER (PARTITION BY id ORDER BY lsn
DESC, ts_ms DESC)`. The WAL LSN is monotonic and unique per change.

### `ecommerce_lakehouse.silver` invalid schema name
**Symptom:** dbt produces an invalid schema like `ecommerce_lakehouse.silver`.
**Cause:** dbt's default `generate_schema_name` prepends the profile schema.
**Fix:** Override `generate_schema_name` to return the configured schema as-is.

### Referential test fails on streaming tail lag
**Symptom:** `order_items.order_id -> orders` relationship test fails because
the newest order lines reference orders not yet landed.
**Cause:** `orders` and `order_items` are independent streams; their tails can
lead each other (eventual consistency).
**Fix:** Set the relationship test to `warn_if: ">0"`, `error_if: ">500"` —
tolerate normal lag, fail only on structural breakage.

---

## Stock Monitor (Kafka consumer)

### No alerts despite low stock
**Symptom:** `stock_monitor` starts cleanly, logs the banner, but never emits a
low-stock alert even after manually setting `stock_qty` below the threshold.
**Cause:** The consumer used `auto_offset_reset="latest"`, which only reads
events that arrive *after* the consumer joins. Manual UPDATEs landed during the
join/rebalance window and were skipped.
**Diagnosis steps that isolated it:**
1. Confirmed the topic exists: `kafka-topics --list | grep ecom` → `ecom.public.inventory` present.
2. Confirmed Debezium writes valid envelopes: `kafka-console-consumer --from-beginning` showed `payload.after.stock_qty`.
3. Ran an in-container consumer with `auto_offset_reset='earliest'` and a fresh group — it read all messages fine. This proved the connection, deserializer, and payload extraction all worked; only the offset reset was wrong.
**Fix:** Default to `auto_offset_reset="earliest"` (configurable via
`AUTO_OFFSET_RESET` env var) so the service catches inventory changes it missed
while down.

### Resetting a consumer group's offset
To make an existing group re-read from the beginning, the group must be
**inactive** first. Stop the consumer, wait for the session to time out
(~30s), then delete the group:
```bash
docker compose stop stock-monitor
sleep 30   # wait for Kafka to see the group as empty
docker exec ecom-kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --delete --group stock-monitor-service
docker compose up -d stock-monitor
```
**Gotchas:**
- `--delete` fails with `GroupNotEmptyException` if a consumer is still joined
  (or rejoined). Stop the container and wait before deleting.
- `--reset-offsets` fails with "group is Stable" if the group is active. Same
  rule — the group must be inactive.
- Don't `docker compose up --build` before deleting: the rebuilt container
  rejoins the group and makes it non-empty again.

### `kafka-console-consumer` can't connect to `localhost:9092`
**Symptom:** `Connection to node -1 (localhost/127.0.0.1:9092) could not be
established. Broker may not be available.`
**Cause:** Inside the Kafka container the broker's advertised listener is
`kafka:9092`, not `localhost:9092`.
**Fix:** Use `--bootstrap-server kafka:9092` for in-container commands.

### Inventory stock goes negative
**Symptom:** `stock_qty` values like `-117`, `-190` in bronze.
**Cause:** `generate.py` decrements stock without checking availability —
there is no "insufficient stock" guard. In a real system the application (OLTP)
would reject the order in-transaction.
**Fix:** Not a pipeline bug — it's a property of the simulator. It actually
demonstrates the architecture correctly: stock control is the application's
job; CDC only observes and reports the result faithfully.

---

## Docker / Environment

### Stopping without losing data
- `docker compose stop` — stop containers, keep data.
- `docker compose down` — remove containers, **keep** named volumes (data).
- `docker compose down -v` — remove everything **including data**. Use with care.

### MinIO bucket missing after `down -v`
**Symptom:** Pipeline fails because the `lakehouse` bucket doesn't exist.
**Cause:** `down -v` wipes the MinIO volume.
**Fix:** A `minio-init` service auto-creates the bucket on startup via
`mc mb --ignore-existing local/lakehouse`; pyspark/spark-thrift depend on it
with `service_completed_successfully`.

### Rebuilt container runs old code
**Symptom:** Code change pushed and pulled, but the container still runs the
old behavior.
**Cause:** Docker build cache reused a stale layer (e.g. the `COPY` layer was
cached).
**Fix:** Rebuild without cache:
```bash
docker compose build --no-cache <service>
docker compose up -d <service>
```

### Cleaning up old images reclaims a lot of space
`docker image prune -a` removes images not attached to a **running** container.
Note this can remove your project's own images if its containers are stopped —
they'll be rebuilt automatically on the next `up`. (Reclaimed ~6.9 GB once.)

### Partial reset: Postgres wiped but Kafka / checkpoints kept (stream stalls)
**Symptom:** The generator writes to Postgres (row count climbs, e.g. max
order_id grows) but PySpark logs no new `Batch N` lines and bronze stays frozen
at an old count. The Debezium connector reports `RUNNING`. Querying the Kafka
topic offset shows it stuck at the old high-water mark; reading the last
messages shows old order_ids and yesterday's timestamps, not the new rows.
**Cause:** The volumes got reset unevenly. Postgres's volume was wiped (fresh
DB, low order_ids, new LSN space) while Kafka's `kafka_data` and the PySpark
checkpoints on MinIO survived. Three pieces now disagree:
1. Debezium's replication slot remembers an old LSN from the previous DB, so it
   ignores the new (low-LSN) changes from the fresh Postgres — no new events
   reach Kafka even though the connector is "running".
2. Kafka still holds the old messages, so its end offset doesn't move.
3. PySpark's checkpoint points at that same old offset, sees nothing newer, and
   never triggers a batch.
**How to confirm:**
```bash
# source is producing?
docker exec ecom-postgres psql -U postgres -d ecommerce -c "SELECT COUNT(*), MAX(order_id) FROM orders;"
# kafka end offset (stuck if it doesn't grow)
docker exec ecom-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic ecom.public.orders
# last messages — old ids / old timestamps reveal the mismatch
docker exec ecom-kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic ecom.public.orders --offset <end-3> --partition 0 --max-messages 3 --timeout-ms 8000
```
**Fix:** Reset everything together so all state lines up again:
```bash
docker compose down -v   # wipes Postgres, Kafka, MinIO, checkpoints
docker compose up -d      # fresh snapshot, empty Kafka, clean checkpoints
```
**Lesson:** Postgres, Kafka, and the PySpark checkpoint hold *coupled* state.
Resetting one without the others breaks CDC. Either reset all of them
(`down -v`) or none. In production this is why slot management and coordinated
recovery matter — you can't restore one component to an old point in time in
isolation.

### WSL2 / Docker Desktop crashes (Windows)
- Recover with `wsl --shutdown`, then restart Docker Desktop.
- Named volumes survive, so no data loss.
- Windows forced restarts (Windows Update) kill running containers — set
  "active hours" to avoid surprise reboots.

---

## Git (cross-machine workflow)

### Push rejected: "Updates were rejected because the remote contains work..."
**Cause:** The other machine pushed commits this machine doesn't have.
**Fix:** `git pull --rebase` then `git push`. Always `git pull` at the start of
a session when working across two machines.

### `order_id` doesn't reset on container restart
Not a bug — `order_id` is `BIGSERIAL`, so Postgres continues the sequence from
where it left off. The log numbers reflect real database IDs, not a per-run
counter.
