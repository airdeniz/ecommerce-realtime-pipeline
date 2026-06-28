# E-Commerce Real-Time Pipeline

A real-time e-commerce data pipeline built with open-source tools on a self-hosted lakehouse. Designed as a portfolio project targeting Turkish e-commerce companies (Trendyol, n11, Hepsiburada).

## What This Project Solves

Modern e-commerce companies need to answer questions like *"How much revenue did we make today?"*, *"Which products are trending right now?"*, or *"How many orders got cancelled in the last hour?"* — and they need answers fast, without slowing down the production database.

This pipeline shows how to do that end-to-end:

- **Capture every change** in the operational database (Postgres) the moment it happens, without polling tables or impacting performance — using **CDC** via Debezium.
- **Decouple producers from consumers** with Kafka, so you can add new downstream systems (analytics, ML, search) without touching the source database.
- **Store raw data cheaply but reliably** in a self-hosted lakehouse (MinIO + Iceberg) — same benefits as Snowflake or Databricks, no cloud vendor lock-in.
- **Transform data in layers** (bronze → silver → gold) with dbt, so analysts get clean, business-ready tables and engineers keep raw data available for reprocessing.
- **Run transformations on a schedule** with Airflow, so the analytics tables are always fresh by morning.
- **Visualize results** with Superset, so business users see charts instead of SQL.

In short: it shows how to build the **same data infrastructure that companies like Trendyol, Hepsiburada or n11 run in production** — but with open-source tools and a single `docker compose up`.

## High-Level Architecture

```mermaid
flowchart LR
    GEN[Order Generator] -->|INSERT| PG[(Postgres<br/>WAL)]
    PG -->|CDC| DBZ[Debezium]
    DBZ -->|JSON events| KAFKA[Kafka]
    KAFKA -->|stream| SPARK[PySpark]
    SPARK -->|Iceberg write| MINIO[(MinIO<br/>Lakehouse)]
    MINIO -->|read| THRIFT[Spark Thrift Server]
    THRIFT --> DBT[dbt Core]
    DBT -->|transform| MINIO
    AIRFLOW[Airflow<br/>nightly DAG] -.->|trigger| DBT
    THRIFT --> SUPERSET[Superset Dashboard]
    CONSOLE[Redpanda Console] -.->|monitor| KAFKA
```

## Low-Level Data Flow

```mermaid
flowchart TB
    subgraph SOURCE["Source Layer"]
        G[generate.py<br/>random orders every 5s]
        P[("Postgres<br/>users, products, orders,<br/>order_items, inventory<br/>REPLICA IDENTITY FULL")]
        G -->|psycopg2 INSERT| P
    end

    subgraph CDC_LAYER["CDC Layer"]
        D[Debezium Connect<br/>pgoutput plugin]
        K[("Kafka KRaft<br/>topics: ecom.public.*")]
        P -->|WAL logical replication| D
        D -->|Debezium JSON<br/>payload.before / payload.after| K
    end

    subgraph STREAM["Stream Processing"]
        S[PySpark Structured Streaming<br/>orders_stream.py]
        K -->|subscribe earliest| S
        S -->|parse JSON, filter op c/u/r| S
    end

    subgraph LAKEHOUSE["Lakehouse - MinIO"]
        B[("Bronze<br/>lakehouse.bronze.*<br/>Iceberg tables")]
        ST[("Staging - views<br/>ecommerce_staging")]
        SL[("Silver - tables<br/>ecommerce_silver<br/>core_orders, core_order_items")]
        GD[("Gold - tables<br/>ecommerce_gold<br/>mart_daily_revenue<br/>mart_sales_by_category")]
        S -->|writeStream foreachBatch| B
        B --> ST
        ST --> SL
        SL --> GD
    end

    subgraph ORCH["Orchestration"]
        A[Airflow DAG<br/>cron: 0 2 * * *]
        TH[Spark Thrift Server<br/>port 10000]
        DB[dbt Core 1.8]
        A -->|trigger| DB
        DB -->|HiveServer2 protocol| TH
        TH -->|Iceberg SQL| LAKEHOUSE
    end

    subgraph VIZ["Visualization"]
        SUP[Superset<br/>Postgres metadata]
        SUP -->|hive://spark-thrift:10000| TH
    end
```

## Component Breakdown

### Source Layer

**Order Generator (`generator/generate.py`)**
Python script using `psycopg2` to simulate real e-commerce traffic. Inserts random orders, users, products, and order items into Postgres every few seconds.
*Why:* You need a continuous source of data changes to demonstrate a real-time pipeline.

**Postgres 16 (`postgres/`)**
Operational database with WAL (Write-Ahead Log) enabled at the logical level. Every INSERT/UPDATE/DELETE is recorded in the WAL.
*Why:* Postgres is the only OLTP database in this pipeline. CDC works by reading the WAL, so it must be configured with `wal_level=logical` and `REPLICA IDENTITY FULL`.

### CDC Layer

**Debezium 2.6 (`debezium/`)**
Kafka Connect plugin that reads Postgres WAL via the `pgoutput` plugin and publishes change events to Kafka. Registered automatically at startup via `connector-init` service hitting the Debezium REST API.
*Why:* CDC enables capturing changes without polling tables. Zero load on the source database.

**Kafka (KRaft mode)**
Message broker that decouples the producer (Debezium) from consumers (PySpark). Topics: `ecom.public.orders`, `ecom.public.users`, `ecom.public.products`, `ecom.public.order_items`.
*Why:* Without Kafka, every downstream consumer would have to connect directly to Postgres. Kafka acts as a durable buffer with multiple consumer support.

**Redpanda Console**
Web UI for inspecting Kafka topics, messages, and connector status.
*Why:* Debugging streaming pipelines without a UI is painful. This is the "DevTools" of Kafka.

### Stream Processing

**PySpark 3.5.1 (`pyspark/orders_stream.py`)**
Structured Streaming job that:
1. Subscribes to all `ecom.public.*` topics from earliest offset
2. Parses the Debezium JSON payload
3. Filters to only `create`, `update`, `read` operations
4. Writes to Iceberg bronze tables in MinIO

*Why:* Kafka events are raw Debezium JSON. We need transformation logic and Iceberg format support — that's what Spark provides. A Kafka Connect S3 sink would only dump raw JSON.

### Lakehouse

**MinIO**
S3-compatible object storage. Holds Iceberg table files (Parquet data + metadata JSON).
*Why:* Self-hosted alternative to AWS S3. The storage layer of the lakehouse.

**Apache Iceberg**
Open table format providing ACID transactions, schema evolution, time travel, and partition evolution on top of object storage.
*Why:* Without Iceberg, MinIO would just hold raw Parquet files with no transaction guarantees. Iceberg makes a data lake behave like a data warehouse.

**Iceberg Catalog — JDBC over Postgres (`iceberg-db`)**
The catalog tracks the current metadata pointer for every Iceberg table. This project uses a **JDBC catalog** backed by a dedicated Postgres instance instead of the simpler Hadoop catalog.
*Why:* The Hadoop catalog stores the metadata pointer as a file in object storage and commits by renaming it. On S3/MinIO, rename is **not atomic** and there is **no locking**, so two concurrent writers can clobber each other's commits — and here the streaming job writes bronze continuously while dbt writes silver/gold. A JDBC catalog turns each commit into an atomic Postgres transaction, which is the production-safe way to coordinate concurrent Iceberg writers without standing up a full Hive Metastore.

**Spark Thrift Server**
JDBC/ODBC endpoint exposing Spark SQL on port 10000.
*Why:* `spark-submit` runs batch jobs. Thrift Server keeps Spark running so dbt and Superset can connect and run SQL on demand via the HiveServer2 protocol.

**dbt Core 1.8 (`dbt/`)**
Transformation layer running SQL models in three layers:
- `staging` → views (ecommerce_staging)
- `core` → silver tables (ecommerce_silver)
- `mart` → gold tables (ecommerce_gold)

*Why:* Raw bronze data isn't analytics-ready. dbt provides modeling, testing, documentation, and lineage — the industry standard.

### Orchestration

**Apache Airflow 2.9 (`airflow/`)**
Runs `dbt_pipeline` DAG every night at 02:00. Two tasks: `dbt_run` → `dbt_test`.
*Why:* Streaming is continuous (PySpark) but transformations are batch. Airflow ensures dbt runs reliably on schedule with retries, logging, and observability.

### Visualization

**Apache Superset**
Connected to Spark Thrift via `hive://spark-thrift:10000`. Reads from `ecommerce_gold` tables.
*Why:* Closes the loop — business users see charts, not SQL. Metadata stored in a dedicated Postgres database (`superset-db`) for persistence across restarts.

## Data Layers

| Layer | Schema | Storage | Updated By |
|-------|--------|---------|------------|
| Bronze | `lakehouse.bronze` | Iceberg (MinIO) | PySpark streaming (continuous) |
| Staging | `ecommerce_staging` | Iceberg views | dbt (nightly) |
| Silver | `ecommerce_silver` | Iceberg tables | dbt (nightly) |
| Gold | `ecommerce_gold` | Iceberg tables | dbt (nightly) |

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
the stale `CREATED` row. Downstream, `core_orders` filters out `CREATED`
orders — so those orders silently disappear and **revenue is undercounted**.

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

## Project Phases

- [x] Phase 1 — CDC Pipeline: Postgres + Debezium + Kafka + Order Generator
- [x] Phase 2 — Stream Processing: PySpark → MinIO (Iceberg)
- [x] Phase 3 — Lakehouse: dbt (staging → silver → gold)
- [x] Phase 4 — Orchestration: Airflow DAG (nightly dbt run)
- [x] Phase 5 — Dashboard: Superset
- [x] Phase 6 — Persistence: Kafka + Superset Postgres metadata

## Getting Started

### Prerequisites

- Docker + Docker Compose
- 16GB+ RAM recommended

### Run

```bash
git clone https://github.com/airdeniz/ecommerce-realtime-pipeline.git
cd ecommerce-realtime-pipeline
cp .env.example .env
docker compose up -d
```

### Initialize Superset (first run only)

```bash
docker exec ecom-superset superset db upgrade
docker exec ecom-superset superset init
docker exec ecom-superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin
```

Then connect Superset to Spark Thrift Server:
- Settings → Database Connections → + Database → Apache Hive
- SQLAlchemy URI: `hive://spark-thrift:10000`

## Services

| Service | URL | Credentials | Volume |
|---------|-----|-------------|--------|
| Redpanda Console | http://localhost:8081 | — | — |
| Airflow | http://localhost:8082 | admin / admin | `airflow_db_data` |
| Debezium REST API | http://localhost:8083 | — | — |
| Superset | http://localhost:8088 | admin / admin | `superset_db_data` |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 | `minio_data` |
| Spark Thrift Server | localhost:10000 | — | — |
| Postgres | localhost:5433 | postgres / postgres | — |
| Iceberg Catalog DB | internal only | iceberg / iceberg | `iceberg_db_data` |
| Kafka | localhost:29092 | — | `kafka_data` |

> Debezium connector is registered automatically on startup via the `connector-init` service.
> `docker compose down` (without `-v`) preserves all data via named volumes.