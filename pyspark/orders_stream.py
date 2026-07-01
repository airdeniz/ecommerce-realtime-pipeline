import sys
import os
sys.stdout.reconfigure(line_buffering=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, get_json_object

MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
ICE_USER = os.environ.get("ICEBERG_DB_USER", "iceberg")
ICE_PASS = os.environ.get("ICEBERG_DB_PASSWORD", "iceberg")
ICE_DB = os.environ.get("ICEBERG_DB_NAME", "iceberg")

spark = SparkSession.builder \
    .appName("ecommerce-orders-stream") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.lakehouse.uri", f"jdbc:postgresql://iceberg-db:5432/{ICE_DB}") \
    .config("spark.sql.catalog.lakehouse.jdbc.user", ICE_USER) \
    .config("spark.sql.catalog.lakehouse.jdbc.password", ICE_PASS) \
    .config("spark.sql.catalog.lakehouse.jdbc.schema-version", "V1") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Iceberg bronze tables: RAW payload approach.
# Every table shares the same minimal schema:
#   op, lsn, ts_ms  -> CDC metadata (for dedup and ordering)
#   <pk>            -> dedup partition key (a separate column instead of
#                      extracting it from JSON each time -> performance + clean PARTITION BY)
#   raw_payload     -> the ENTIRE Debezium payload, as a JSON string
#
# WHY: when a new column is added to the source table, that column is captured
# automatically without changing the bronze schema at all (it arrives inside
# raw_payload). This upholds the "store everything even if unused" principle:
# if a column is needed later, it is already present in historical data too.
# Fields are extracted in staging via get_json_object / JSON path. Trade-off:
# JSON parse cost on read, but bronze is the "capture-and-store" layer;
# interpretation happens in the upper layers.
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        order_id BIGINT,
        raw_payload STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.users (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        user_id BIGINT,
        raw_payload STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.products (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        product_id BIGINT,
        raw_payload STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_items (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        order_item_id BIGINT,
        raw_payload STRING
    ) USING iceberg
""")

# Single, delete-aware stream factory for all tables.
# Output: op, lsn, ts_ms, <pk>, raw_payload
#   - op/lsn/ts_ms: CDC metadata (ordering + dedup)
#   - pk: dedup partition key; since after is NULL on a delete it is taken via
#         COALESCE(after.<pk>, before.<pk>)
#   - raw_payload: the ENTIRE payload.after as a raw JSON string (on a delete
#         after is NULL, so before is used).
#
# IMPORTANT: the Kafka value is NEVER parsed into a StructType. If we parsed it
# with a fixed schema (from_json), a new column not in the schema (e.g. discount)
# would be DROPPED during parsing and never reach raw_payload -> "capture
# everything" would break. Instead we pull the needed fields raw from JSON paths
# via get_json_object, and take after/before as raw JSON strings too. This way
# every new column added to the source ends up in raw_payload automatically,
# with no schema change.
def make_stream(topic, pk):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(col("value").cast("string").alias("v")) \
        .select(
            get_json_object(col("v"), "$.payload.op").alias("op"),
            get_json_object(col("v"), "$.payload.source.lsn").cast("long").alias("lsn"),
            get_json_object(col("v"), "$.payload.source.ts_ms").cast("long").alias("ts_ms"),
            coalesce(
                get_json_object(col("v"), f"$.payload.after.{pk}"),
                get_json_object(col("v"), f"$.payload.before.{pk}"),
            ).cast("long").alias(pk),
            coalesce(
                get_json_object(col("v"), "$.payload.after"),
                get_json_object(col("v"), "$.payload.before"),
            ).alias("raw_payload"),
        ) \
        .filter(col("op").isin("c", "u", "r", "d"))

orders_df = make_stream("ecom.public.orders", "order_id")
users_df = make_stream("ecom.public.users", "user_id")
products_df = make_stream("ecom.public.products", "product_id")
order_items_df = make_stream("ecom.public.order_items", "order_item_id")

def write_to_iceberg(table_name):
    def inner(batch_df, batch_id):
        batch_df.persist()
        try:
            n = batch_df.count()
            if n > 0:
                batch_df.writeTo(table_name).append()
                print(f"[{table_name}] Batch {batch_id}: {n} rows written")
        finally:
            batch_df.unpersist()
    return inner

q1 = orders_df.writeStream.foreachBatch(write_to_iceberg("lakehouse.bronze.orders")) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/orders").start()

q2 = users_df.writeStream.foreachBatch(write_to_iceberg("lakehouse.bronze.users")) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/users").start()

q3 = products_df.writeStream.foreachBatch(write_to_iceberg("lakehouse.bronze.products")) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/products").start()

q4 = order_items_df.writeStream.foreachBatch(write_to_iceberg("lakehouse.bronze.order_items")) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/order_items").start()

spark.streams.awaitAnyTermination()
