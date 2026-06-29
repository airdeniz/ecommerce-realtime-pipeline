import sys
import os
sys.stdout.reconfigure(line_buffering=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, coalesce, to_json, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType

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
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Debezium source metadata: lsn = WAL log sequence number (monoton artan,
# her degisiklik icin tekil -> CDC olaylarini siralamanin kanonik yolu).
# ts_ms = commit zamani (lsn yoksa yedek siralama anahtari).
source_schema = StructType([
    StructField("lsn", LongType()),
    StructField("ts_ms", LongType()),
])

# ORDERS
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("status", StringType()),
    StructField("total_amount", StringType()),
    StructField("created_at", StringType()),
])

debezium_order_schema = StructType([
    StructField("payload", StructType([
        StructField("before", order_schema),
        StructField("after", order_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
    ]))
])

# USERS
user_schema = StructType([
    StructField("user_id", LongType()),
    StructField("full_name", StringType()),
    StructField("city", StringType()),
    StructField("created_at", StringType()),
])

debezium_user_schema = StructType([
    StructField("payload", StructType([
        StructField("before", user_schema),
        StructField("after", user_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
    ]))
])

# PRODUCTS
product_schema = StructType([
    StructField("product_id", LongType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", StringType()),
])

debezium_product_schema = StructType([
    StructField("payload", StructType([
        StructField("before", product_schema),
        StructField("after", product_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
    ]))
])

# ORDER ITEMS
order_item_schema = StructType([
    StructField("order_item_id", LongType()),
    StructField("order_id", LongType()),
    StructField("product_id", LongType()),
    StructField("quantity", LongType()),
    StructField("unit_price", StringType()),
])

debezium_order_item_schema = StructType([
    StructField("payload", StructType([
        StructField("before", order_item_schema),
        StructField("after", order_item_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
    ]))
])

# Iceberg bronze tablolari: HAM (raw) payload yaklasimi.
# Her tablo ayni minimal sema:
#   op, lsn, ts_ms  -> CDC metadata (dedup ve siralama icin)
#   <pk>            -> dedup partition anahtari (JSON'dan her seferinde cikarmak
#                      yerine ayri kolon -> performans + temiz PARTITION BY)
#   raw_payload     -> Debezium payload'inin TAMAMI, JSON string olarak
#
# NEDEN: kaynak tabloya yeni bir kolon eklendiginde bronze semasini hic
# degistirmeden o kolon otomatik yakalanir (raw_payload icinde gelir).
# Boylece "kullanmasak bile her seyi sakla" prensibi saglanir: ileride bir
# kolona ihtiyac olursa gecmis veride de mevcuttur. Alanlar staging'de
# get_json_object / JSON path ile cikarilir. Trade-off: okurken JSON parse
# maliyeti, ama bronze "yakala-sakla" katmani; anlamlandirma yukari katmanda.
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

# Tum tablolar icin tek, delete-aware stream uretici.
# Cikti: op, lsn, ts_ms, <pk>, raw_payload
#   - op/lsn/ts_ms: CDC metadata (siralama + dedup)
#   - pk: dedup partition anahtari; delete'te after NULL oldugu icin
#         COALESCE(after.<pk>, before.<pk>) ile alinir
#   - raw_payload: payload.after'in TAMAMI JSON string (delete'te after NULL
#         oldugundan before kullanilir). Tum is kolonlari burada; yeni kolon
#         eklendiginde otomatik yakalanir.
def make_stream(topic, schema, pk):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select(
            col("data.payload.op").alias("op"),
            col("data.payload.source.lsn").alias("lsn"),
            col("data.payload.source.ts_ms").alias("ts_ms"),
            coalesce(col(f"data.payload.after.{pk}"), col(f"data.payload.before.{pk}")).alias(pk),
            coalesce(to_json(col("data.payload.after")), to_json(col("data.payload.before"))).alias("raw_payload"),
        ) \
        .filter(col("op").isin("c", "u", "r", "d"))

orders_df = make_stream("ecom.public.orders", debezium_order_schema, "order_id")
users_df = make_stream("ecom.public.users", debezium_user_schema, "user_id")
products_df = make_stream("ecom.public.products", debezium_product_schema, "product_id")
order_items_df = make_stream("ecom.public.order_items", debezium_order_item_schema, "order_item_id")

def write_to_iceberg(table_name):
    def inner(batch_df, batch_id):
        batch_df.persist()
        try:
            n = batch_df.count()
            if n > 0:
                batch_df.writeTo(table_name).append()
                print(f"[{table_name}] Batch {batch_id}: {n} kayit yazildi")
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
