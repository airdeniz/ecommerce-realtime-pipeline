# E-Commerce Real-Time Pipeline

Real-time e-commerce data pipeline using Docker, CDC, Kafka, Flink, dbt and Iceberg on a self-hosted lakehouse.

## Architecture

Postgres → Debezium (CDC) → Kafka → PySpark → MinIO (Iceberg) → dbt → Superset

## Stack

| Tool | Role |
|------|------|
| Postgres | Operational database |
| Debezium | CDC — captures row-level changes from Postgres WAL |
| Kafka (KRaft) | Message broker |
| Redpanda Console | Kafka UI |
| PySpark | Stream processing |
| MinIO | S3-compatible object storage (lakehouse) |
| Apache Iceberg | Open table format |
| dbt Core | Transformation (staging → core → mart) |
| Airflow | Orchestration |
| Superset | Dashboard |

## Project Phases

- [x] Phase 1 — CDC Pipeline: Postgres + Debezium + Kafka + Order Generator
- [x] Phase 2 — Stream Processing: PySpark
- [x] Phase 3 — Lakehouse: MinIO + Iceberg + dbt (MinIO + Iceberg ✓, dbt pending)
- [x] Phase 4 — Orchestration: Airflow
- [ ] Phase 5 — Dashboard: Superset

## Getting Started

### Prerequisites
- Docker + Docker Compose

### Run

```bash
git clone https://github.com/airdeniz/ecommerce-realtime-pipeline.git
cd ecommerce-realtime-pipeline
cp .env.example .env
docker compose up -d
```

### Verify

Open Redpanda Console at `http://localhost:8081` — you should see `ecom.public.orders` topic receiving messages.

### Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Redpanda Console | http://localhost:8081 | - |
| Airflow | http://localhost:8082 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| Debezium REST API | http://localhost:8083 | - |

> Debezium connector is registered automatically on startup via `connector-init` service.