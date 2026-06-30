# Runbook

## Quick Commands (cheat sheet)

```bash
# Start everything from scratch (build images first)
docker compose up -d --build

# Resume after a stop (continue with existing data, fast — no rebuild)
docker compose start

# Pause everything, keep all data (resume later with `start`)
docker compose stop

# FREEZE data generation only — keep query/dashboard/AI layer running.
# Use this to query, build Superset dashboards, and ask the AI agent
# against a fixed dataset that no longer grows.
docker compose stop generator pyspark stock-monitor connect
#   ...resume data flow later with:
docker compose start generator pyspark stock-monitor connect

# FULL RESET — delete containers AND volumes (wipes all data, fresh schema)
docker compose down -v
docker compose up -d --build

# Run dbt to (re)build staging -> silver -> gold -> ml_features (after data
# flows / a reset). Healthy run builds 11 models: PASS=11 WARN=0 ERROR=0.
docker exec ecom-airflow-scheduler dbt run \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

---

Common operational commands for running, resetting, querying, and debugging the
pipeline. For first-time setup see the [README](README.md); for problems and
their fixes see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

> Paths use the container names (`ecom-*`). On Windows PowerShell use `curl.exe`
> (not `curl`) and full download paths (e.g. `C:\Users\deniz\Downloads\x.patch`),
> since `~` does not expand.
>
> **Note (Windows PowerShell 5.1):** the `&&` chaining used in some commands below
> (e.g. `docker compose down -v && docker compose up -d --build`) is not supported —
> run the two commands separately, or use `;` if you don't care whether the first
> one succeeds.

## Lifecycle

```bash
docker compose up -d                 # start everything (detached)
docker compose up -d --build         # rebuild images first (after code changes)
docker compose stop                  # stop, keep volumes (quick pause)
docker compose start                 # resume after stop
docker compose down                  # remove containers, keep volumes
docker compose down -v               # remove containers AND volumes (full reset)
docker compose restart pyspark       # restart a single service
docker compose ps                    # status of all services
```

**Full reset (most common fix).** Postgres, Kafka, and the Spark checkpoints
are coupled state — if a schema or pipeline change makes them disagree, reset
them together:

```bash
docker compose down -v
docker compose up -d --build
```

After a reset, wait ~2-3 minutes for PySpark to populate bronze, then run dbt.

## dbt (transformations)

```bash
# run all models (bronze -> staging -> silver -> gold)
docker exec ecom-airflow-scheduler dbt run \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# run the tests
docker exec ecom-airflow-scheduler dbt test \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# run a single model
docker exec ecom-airflow-scheduler dbt run --select stg_orders \
  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

A healthy run builds 11 models (8 staging/silver/gold + 3 `ml_features`) and
ends with `PASS=11 WARN=0 ERROR=0`.

## Machine Learning layer

The ML jobs read `lakehouse.ml_features.*` (built by dbt above) and write model
outputs to `lakehouse.ml.*`. Each job runs a local Spark engine inside the
scheduler (no separate Spark cluster). Run the whole pipeline via Airflow:

```bash
docker exec ecom-airflow-scheduler airflow dags trigger ml_pipeline
```

Or run a single job by hand (the helper builds the baked --jars list):

```bash
docker exec ecom-airflow-scheduler bash -c \
  'JARS=$(ls /opt/ml-jars/*.jar | tr "\n" "," | sed "s/,$//"); \
   spark-submit --master "local[*]" --driver-memory 2g --jars "$JARS" \
   --py-files /opt/airflow/ml/common.py /opt/airflow/ml/fraud_isolation_forest.py'
#   ...swap in demand_forecast.py / customer_segmentation.py / churn_prediction.py
```

Inspect the results:

```bash
docker exec -it ecom-mcp-server python -c \
  "import server; print(server.run_query('SELECT * FROM lakehouse.ml.fraud_scores ORDER BY anomaly_score DESC LIMIT 10'))"
docker exec -it ecom-mcp-server python -c \
  "import server; print(server.run_query('SELECT segment_label, COUNT(*) FROM lakehouse.ml.customer_segments GROUP BY segment_label'))"
```

> The ML jobs depend on `lakehouse.ml_features` existing — run `dbt run` first
> (the nightly `dbt_pipeline` DAG builds it at 02:00; `ml_pipeline` runs at 03:00).

### Superset — "ML Insights" dashboard

The ML tables are queryable over the **same** Spark Thrift connection Superset
already uses — no new database connection. To build the dashboard:

1. **Data → Datasets → + Dataset**, pick the existing Spark Thrift database,
   schema `lakehouse.ml`, and add each table:
   `fraud_scores`, `demand_forecast`, `customer_segments`, `churn_predictions`.
2. Suggested charts:
   - *Flagged orders* — table/bar on `fraud_scores` filtered `is_anomaly = true`,
     sorted by `anomaly_score` desc.
   - *Revenue forecast* — time-series line on `demand_forecast` (`ds` vs `yhat`,
     band `yhat_lower`/`yhat_upper`), filtered by `grain = 'daily'` or `'hourly'`.
   - *Segments* — pie/bar of customer counts by `segment_label`; scatter of
     `frequency` vs `monetary` coloured by segment.
   - *Churn risk* — table of top users by `churn_probability`.
3. Add the charts to a new **"ML Insights"** dashboard. Superset metadata persists
   in `superset_db_data`, so it survives restarts (but not `down -v`).

## Querying the lakehouse

Via Beeline (inside the Thrift container):

```bash
docker exec -it ecom-spark-thrift /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000"
# then:  SHOW NAMESPACES IN lakehouse;
#        SELECT * FROM lakehouse.gold.mart_sales_by_category;
#        !quit
```

Via the MCP server's tools (handy for a quick check):

```bash
docker exec -it ecom-mcp-server python -c "import server; print(server.list_tables())"
docker exec -it ecom-mcp-server python -c "import server; print(server.describe_table('lakehouse.bronze.orders'))"
docker exec -it ecom-mcp-server python -c "import server; print(server.run_query('SELECT * FROM lakehouse.gold.mart_sales_by_category'))"
```

## Health checks

```bash
# source DB producing? (row count / max id should climb)
docker exec ecom-postgres psql -U postgres -d ecommerce -c "SELECT COUNT(*), MAX(order_id) FROM orders;"

# Debezium connector registered and RUNNING?
curl.exe http://localhost:8083/connectors
curl.exe http://localhost:8083/connectors/ecommerce-connector/status

# Kafka topic end offset (should grow as data flows)
docker exec ecom-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic ecom.public.orders

# PySpark writing to bronze? (look for "Batch N: M kayit yazildi")
docker logs ecom-pyspark --tail 20

# generator emitting (and occasionally deleting) orders?
docker logs ecom-generator --tail 20
```

## Demonstrating features

```bash
# Soft delete: delete a cancelled order, watch op='d' reach bronze
docker exec ecom-postgres psql -U postgres -d ecommerce -c \
  "SELECT order_id FROM orders WHERE status='CANCELLED' LIMIT 1;"
docker exec ecom-postgres psql -U postgres -d ecommerce -c \
  "DELETE FROM order_items WHERE order_id = <ID>; DELETE FROM orders WHERE order_id = <ID>;"

# Schema evolution: add a column, update a row twice (first update refreshes
# Debezium's schema cache), then see it appear in bronze raw_payload untouched
docker exec ecom-postgres psql -U postgres -d ecommerce -c \
  "ALTER TABLE orders ADD COLUMN discount NUMERIC(10,2) DEFAULT 0;"
docker exec ecom-postgres psql -U postgres -d ecommerce -c \
  "UPDATE orders SET discount = 200 WHERE order_id = 5;"
docker exec ecom-postgres psql -U postgres -d ecommerce -c \
  "UPDATE orders SET discount = 250 WHERE order_id = 5;"
docker exec -it ecom-mcp-server python -c \
  "import server; print(server.run_query('SELECT op, order_id, raw_payload FROM lakehouse.bronze.orders WHERE order_id = 5 ORDER BY lsn DESC LIMIT 2'))"
```

## Connecting the AI agent (Claude Desktop)

Add to the MCP config (`%APPDATA%\Claude\claude_desktop_config.json` on Windows,
`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "lakehouse": {
      "command": "docker",
      "args": ["exec", "-i", "ecom-mcp-server", "python", "server.py"]
    }
  }
}
```

Fully restart Claude Desktop, then ask in a new chat:
*"list the tables in the lakehouse"* or *"which category sold the most?"*.
The pipeline (and `ecom-mcp-server`) must be running.

## Maintenance

```bash
docker image prune -a                 # reclaim space from unused images
docker system df                      # show docker disk usage
docker compose build --no-cache pyspark  # rebuild ignoring cache (stale code)
```
