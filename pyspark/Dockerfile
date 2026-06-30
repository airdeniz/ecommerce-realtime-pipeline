# Apache'nin resmi Spark imajindan basliyoruz. Bu imajda Spark motoru, JVM,
# PySpark ve temel JAR'lar hazir gelir; Spark'i sifirdan kurmuyoruz.
FROM apache/spark:3.5.1

# Kurulum adimlari icin root'a geciyoruz (kutuphane kurmak, dosya indirmek
# yetki ister). Spark imaji normalde guvenlik icin yetkisiz 'spark' kullanicisi
# ile calisir; kurulum bitince tekrar ona donecegiz.
USER root

# Iki is birden yapiliyor:
# 1) pip ile Python tarafina Kafka kutuphanesini ekliyoruz.
# 2) Spark cekirdeginde OLMAYAN JAR'lari Maven deposundan indirip
#    /opt/spark/jars/ altina koyuyoruz. Bu JAR'lar olmadan Spark cekirdegi
#    Kafka'yi, Iceberg'i veya S3/MinIO'yu "tanimaz". Gruplar:
#
#    - spark-sql-kafka + kafka-clients + token-provider + commons-pool2:
#        Spark'in Kafka'dan stream okuyabilmesi icin (pipeline'in girisi).
#    - iceberg-spark-runtime:
#        Spark'in Iceberg tablo formatina yazip okuyabilmesi icin (bronze).
#    - aws-java-sdk-bundle + hadoop-aws:
#        Spark'in MinIO'ya S3 protokolu uzerinden erisebilmesi icin (depolama).
#    - postgresql:
#        Spark'in Postgres'e JDBC ile baglanabilmesi icin.
#
# JAR'lari runtime'da '--packages' ile indirmek yerine imaja GOMUYORUZ; boylece
# container internet olmadan, tekrarlanabilir ve hizli sekilde ayaga kalkar.
RUN pip install --no-cache-dir kafka-python==2.0.2 && \
    curl -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Kurulum bitti; guvenlik icin tekrar yetkisiz 'spark' kullanicisina donuyoruz.
USER spark

# Calisma dizinini ayarlayip kendi kodumuzu (orders_stream.py) imaja kopyaliyoruz.
WORKDIR /app
COPY . .
