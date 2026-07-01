"""
Shared helpers for the ML jobs.

Every ML job runs as a local Spark application (spark-submit --master local[*])
inside the Airflow scheduler. It builds a SparkSession pointed at the SAME
Iceberg JDBC catalog the streaming job uses (so concurrent writers are safe via
the Postgres-backed catalog), reads a feature table into pandas (data is small
at this scale), trains a scikit-learn / Prophet model on the driver, and writes
the result back as an Iceberg table under the `lakehouse.ml` namespace.

The Spark/Iceberg/MinIO config mirrors pyspark/orders_stream.py. The required
JARs are baked into the Airflow image at /opt/ml-jars and added here via
`spark.jars`, so the scripts work both under spark-submit and plain `python`.
"""

import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
ICE_USER = os.environ.get("ICEBERG_DB_USER", "iceberg")
ICE_PASS = os.environ.get("ICEBERG_DB_PASSWORD", "iceberg")
ICE_DB = os.environ.get("ICEBERG_DB_NAME", "iceberg")

ML_JARS = ",".join(sorted(glob.glob("/opt/ml-jars/*.jar")))

# Namespaces the ML layer reads from / writes to.
FEATURES_NS = "lakehouse.ml_features"
ML_NS = "lakehouse.ml"


def build_spark(app_name: str) -> SparkSession:
    """Build a local SparkSession wired to the lakehouse Iceberg catalog."""
    builder = (
        SparkSession.builder.appName(app_name)
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
    )
    # When launched via spark-submit --jars these are already on the classpath;
    # setting spark.jars too makes a plain `python ml/<job>.py` run work as well.
    if ML_JARS:
        builder = builder.config("spark.jars", ML_JARS)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # The JDBC catalog does not auto-create namespaces.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ML_NS}")
    return spark


def read_table(spark: SparkSession, table: str):
    """Read an Iceberg table into a pandas DataFrame (small data at this scale)."""
    return spark.table(table).toPandas()


def write_iceberg(spark: SparkSession, pdf, table: str):
    """Write a pandas DataFrame as an Iceberg table, stamped with scored_at."""
    if pdf is None or len(pdf) == 0:
        print(f"[{table}] no rows to write — skipping")
        return
    sdf = spark.createDataFrame(pdf).withColumn("scored_at", current_timestamp())
    sdf.writeTo(table).using("iceberg").createOrReplace()
    print(f"[{table}] wrote {len(pdf)} rows")
