"""
Stock Monitoring Service
================================================

This service is a Kafka CONSUMER. It does not touch the existing pipeline in any
way; it requires no change to Postgres, Debezium, Kafka or PySpark. It simply
connects to the already-flowing `ecom.public.inventory` topic with a new
consumer group.

IMPORTANT CONCEPTUAL DISTINCTION:
---------------------------------
Checking and DECREMENTING stock is the job of the application (OLTP) side:
  - The customer clicks "Place Order"
  - The backend checks whether stock is sufficient (stock_qty >= requested?)
  - If sufficient, it creates the order + decrements stock (UPDATE inventory ...)
  - If not, it returns an "out of stock" error
This all happens at transaction-time, within milliseconds. CDC is unaware of it.

This service does NOT manage stock; it OBSERVES stock changes. The decision has
already been made and the stock has already been decremented. This service
answers "someone did something with stock — who needs to know about it?":

  1. ALERTING / MONITORING — if stock drops below a critical threshold, notify
     the purchasing team (so they can reorder from the supplier). The app does
     not do this; its job is taking orders, not supply planning.

  2. ANALYTICS — burn-rate analysis: how many units of a product sell per day,
     when will it run out? This is not in the historical OLTP (which only has
     current stock); it is in the CDC event stream.

  3. SYNCHRONIZATION — pushing stock changes to other systems like a marketplace
     integration, a warehouse management system, or a supplier portal. Instead
     of each connecting to the OLTP separately, they read from this topic.

This simple example demonstrates use case (1): a low-stock alert.

ALERT SINKS
-----------
Every low-stock crossing is emitted to the console as an `[ALERT]` line (the
original behaviour, always on). In ADDITION, when WRITE_ICEBERG_ALERTS is enabled
(default), the alert is also persisted append-only to the Iceberg table
`lakehouse.ops.stock_alerts` so it becomes queryable from Spark Thrift / Superset.

The Iceberg write reuses the SAME JDBC-over-Postgres catalog + MinIO S3A config
as pyspark/orders_stream.py and ml/common.py. Atomic commits from the JDBC
catalog make this a safe concurrent writer alongside the bronze stream and dbt.
The Kafka consumer loop below is unchanged and stays on kafka-python; Spark is
used ONLY to append the alert row. An Iceberg write failure is caught and logged
so it can never break the console alert or the consumer loop.
"""

import os
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INVENTORY_TOPIC = os.environ.get("INVENTORY_TOPIC", "ecom.public.inventory")
# An alert is produced when stock drops below this threshold
LOW_STOCK_THRESHOLD = int(os.environ.get("LOW_STOCK_THRESHOLD", "10"))
# earliest = on restart, also reads the stock changes it missed while down
# latest   = reads only new events that arrive while the service is connected
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")

# When enabled, alerts are also persisted to lakehouse.ops.stock_alerts (Iceberg).
WRITE_ICEBERG_ALERTS = os.environ.get("WRITE_ICEBERG_ALERTS", "true").lower() == "true"

# Iceberg / MinIO connection settings — identical to pyspark/orders_stream.py.
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
ICE_USER = os.environ.get("ICEBERG_DB_USER", "iceberg")
ICE_PASS = os.environ.get("ICEBERG_DB_PASSWORD", "iceberg")
ICE_DB = os.environ.get("ICEBERG_DB_NAME", "iceberg")

ALERTS_TABLE = "lakehouse.ops.stock_alerts"

# Simple memory to avoid emitting the same alert repeatedly for one product.
# (In prod this lives in Redis/DB; here in-memory is enough.)
already_alerted = set()

# Lazily-built SparkSession, shared across alerts (built once on first use).
_spark = None


def build_spark():
    """Build a local SparkSession wired to the lakehouse Iceberg catalog.

    Mirrors pyspark/orders_stream.py and ml/common.py so the alert rows land in
    the same JDBC catalog that Spark Thrift / Superset read from.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("stock-monitor-alerts")
        .master("local[1]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.lakehouse.uri", f"jdbc:postgresql://iceberg-db:5432/{ICE_DB}")
        .config("spark.sql.catalog.lakehouse.jdbc.user", ICE_USER)
        .config("spark.sql.catalog.lakehouse.jdbc.password", ICE_PASS)
        .config("spark.sql.catalog.lakehouse.jdbc.schema-version", "V1")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # The JDBC catalog does not auto-create namespaces.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ALERTS_TABLE} (
            product_id BIGINT,
            stock_qty  INT,
            threshold  INT,
            alert_time TIMESTAMP
        ) USING iceberg
        """
    )
    return spark


def get_spark():
    """Return the shared SparkSession, building it on first use."""
    global _spark
    if _spark is None:
        _spark = build_spark()
    return _spark


def write_alert(product_id, stock_qty):
    """Append a single low-stock alert row to lakehouse.ops.stock_alerts.

    Any failure here is caught and logged so it can never break the console
    alert or the Kafka consumer loop.
    """
    if not WRITE_ICEBERG_ALERTS:
        return
    try:
        from pyspark.sql import Row
        from pyspark.sql.functions import current_timestamp

        spark = get_spark()
        row = Row(product_id=int(product_id), stock_qty=int(stock_qty), threshold=LOW_STOCK_THRESHOLD)
        df = spark.createDataFrame([row]).withColumn("alert_time", current_timestamp())
        df.select("product_id", "stock_qty", "threshold", "alert_time") \
            .writeTo(ALERTS_TABLE).append()
        print(f"[ICEBERG] Alert persisted to {ALERTS_TABLE} for product_id={product_id}")
    except Exception as exc:  # noqa: BLE001 — never let the sink break alerting
        print(f"[WARN] Failed to persist alert to {ALERTS_TABLE}: {exc}")


def handle_inventory_event(payload):
    """Extract the stock change from the Debezium envelope and evaluate it."""
    after = payload.get("after")
    if after is None:
        # Delete (op=d) — the product left inventory; we do not care
        return

    product_id = after.get("product_id")
    stock_qty = after.get("stock_qty")

    if product_id is None or stock_qty is None:
        return

    if stock_qty < LOW_STOCK_THRESHOLD:
        if product_id not in already_alerted:
            # In real life a Slack webhook / email / PagerDuty would be called here:
            #   requests.post(SLACK_WEBHOOK, json={"text": ...})
            print(
                f"[ALERT] Stock critically low! "
                f"product_id={product_id} stock_qty={stock_qty} "
                f"(threshold={LOW_STOCK_THRESHOLD}) -> notify purchasing team"
            )
            already_alerted.add(product_id)
            # EK feature: also persist the alert to Iceberg (first crossing only,
            # so the spam-prevention memory also prevents duplicate rows).
            write_alert(product_id, stock_qty)
    else:
        # Stock back to normal (restocked) -> clear the alert memory
        already_alerted.discard(product_id)


def main():
    print(f"Stock monitoring service started.")
    print(f"  Topic            : {INVENTORY_TOPIC}")
    print(f"  Low-stock threshold : {LOW_STOCK_THRESHOLD}")
    print(f"  Consumer group   : stock-monitor-service")
    print(f"  Iceberg sink     : {'on -> ' + ALERTS_TABLE if WRITE_ICEBERG_ALERTS else 'off'}")

    consumer = KafkaConsumer(
        INVENTORY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # Consumer group INDEPENDENT of PySpark -> we read the same topic independently
        group_id="stock-monitor-service",
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    )

    for message in consumer:
        if message.value is None:
            continue
        payload = message.value.get("payload")
        if payload is None:
            continue
        handle_inventory_event(payload)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
