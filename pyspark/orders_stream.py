from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Spark session
spark = SparkSession.builder \
    .appName("ecommerce-orders-stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka'dan gelen orders mesajının after kısmının şeması
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("status", StringType()),
    StructField("total_amount", StringType()),
    StructField("created_at", StringType()),
])

# Debezium mesajının dış şeması
debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("after", order_schema),
        StructField("op", StringType()),
    ]))
])

# Kafka'dan oku
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecom.public.orders") \
    .option("startingOffsets", "latest") \
    .load()

# JSON parse et
parsed = df.select(
    from_json(col("value").cast("string"), debezium_schema).alias("data")
).select(
    col("data.payload.op").alias("op"),
    col("data.payload.after.*")
).filter(col("op").isin("c", "u"))

# Konsola yaz
query = parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()