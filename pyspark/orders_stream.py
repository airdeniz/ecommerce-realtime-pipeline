import sys
sys.stdout.reconfigure(line_buffering=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType

spark = SparkSession.builder \
    .appName("ecommerce-orders-stream") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://iceberg-db:5432/iceberg") \
    .config("spark.sql.catalog.lakehouse.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.lakehouse.jdbc.password", "iceberg") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
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
        StructField("after", order_item_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
    ]))
])

# Iceberg tablolari olustur (lsn + ts_ms ile -> downstream dedup icin)
# JDBC catalog'da namespace otomatik olusmadigi icin once acikca yaratiyoruz.
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        order_id BIGINT, user_id BIGINT,
        status STRING, total_amount STRING, created_at STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.users (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        user_id BIGINT, full_name STRING,
        city STRING, created_at STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.products (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        product_id BIGINT, name STRING,
        category STRING, price STRING
    ) USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_items (
        op STRING, lsn BIGINT, ts_ms BIGINT,
        order_item_id BIGINT, order_id BIGINT,
        product_id BIGINT, quantity BIGINT, unit_price STRING
    ) USING iceberg
""")

def make_stream(topic, schema):
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
            col("data.payload.after.*"),
        ) \
        .filter(col("op").isin("c", "u", "r"))

orders_df = make_stream("ecom.public.orders", debezium_order_schema)
users_df = make_stream("ecom.public.users", debezium_user_schema)
products_df = make_stream("ecom.public.products", debezium_product_schema)
order_items_df = make_stream("ecom.public.order_items", debezium_order_item_schema)

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
