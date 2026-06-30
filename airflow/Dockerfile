FROM apache/airflow:2.9.1

USER root

# git: needed by dbt. JRE + procps: needed to run the ML jobs, which launch a
# local Spark session via spark-submit (local[*]) from inside the scheduler.
# curl: used below to bake the Spark JARs into the image.
RUN apt-get update && \
    apt-get install -y git curl procps openjdk-17-jre-headless && \
    apt-get clean

# Bake the JARs the ML SparkSession needs into the image (same approach and
# coordinates as pyspark/Dockerfile, minus the Kafka jars — the ML jobs read
# Iceberg/MinIO, not Kafka). The ML scripts reference these via /opt/ml-jars/*.
#   - iceberg-spark-runtime : read/write the Iceberg table format
#   - aws-java-sdk-bundle + hadoop-aws : reach MinIO over the S3 protocol
#   - postgresql : connect to the Iceberg JDBC catalog (iceberg-db)
RUN mkdir -p /opt/ml-jars && \
    curl -o /opt/ml-jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -o /opt/ml-jars/aws-java-sdk-bundle-1.12.661.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar && \
    curl -o /opt/ml-jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/ml-jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    chmod -R a+r /opt/ml-jars

USER airflow

# Python deps:
#   - dbt-* and the spark provider: the existing transformation/orchestration stack
#   - pyspark 3.5.1: MUST match the baked Spark JARs (3.5.1) so the ML jobs run
#     a local Spark engine that speaks the same Iceberg runtime
#   - scikit-learn: IsolationForest, KMeans, LogisticRegression
#   - prophet >= 1.1.5: demand forecasting. 1.1.x ships a PRECOMPILED Stan model
#     and does NOT compile cmdstan at install time, so the image build stays fast
#     and reliable (see TROUBLESHOOTING.md for the statsmodels fallback)
#   - pyarrow: fast pandas <-> Spark conversion
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.9.0 \
    dbt-core==1.8.0 \
    dbt-spark[PyHive]==1.8.0 \
    pyspark==3.5.1 \
    scikit-learn \
    "prophet>=1.1.5" \
    pyarrow
