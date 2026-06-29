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
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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
#   - raw_payload: payload.after'in TAMAMI ham JSON string (delete'te after NULL
#         oldugundan before kullanilir).
#
# ONEMLI: Kafka value'su HIC StructType ile parse EDILMIYOR. Onceden sabit
# semayla (from_json) parse etseydik, semada olmayan yeni bir kolon (orn.
# discount) parse sirasinda DUSER ve raw_payload'a hic giremezdi -> "her seyi
# yakala" bozulurdu. Bunun yerine get_json_object ile JSON path'lerden gereken
# alanlari ham olarak cekiyoruz; after/before'i da ham JSON string olarak
# aliyoruz. Boylece kaynaga eklenen her yeni kolon, sema degisikligi olmadan
# otomatik raw_payload'da yer alir.
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
