import sys
sys.stdout.reconfigure(line_buffering=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder \
    .appName("ecommerce-orders-stream") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("status", StringType()),
    StructField("total_amount", StringType()),
    StructField("created_at", StringType()),
])

debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("after", order_schema),
        StructField("op", StringType()),
    ]))
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecom.public.orders") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), debezium_schema).alias("data")
).select(
    col("data.payload.op").alias("op"),
    col("data.payload.after.*")
).filter(col("op").isin("c", "u"))

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders (
        op STRING,
        order_id BIGINT,
        user_id BIGINT,
        status STRING,
        total_amount STRING,
        created_at STRING
    )
    USING iceberg
""")

def write_to_iceberg(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.writeTo("lakehouse.bronze.orders").append()
        print(f"Batch {batch_id}: {batch_df.count()} kayit yazildi")

query = parsed.writeStream \
    .foreachBatch(write_to_iceberg) \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/orders") \
    .start()

query.awaitTermination()