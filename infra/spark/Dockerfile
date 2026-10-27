# Spark 3.5 + Iceberg runtime, baked in so runs never resolve dependencies.
ARG SPARK_VERSION=3.5.9
FROM docker.io/apache/spark:${SPARK_VERSION}-scala2.12-java17-ubuntu

ARG ICEBERG_VERSION=1.11.0

USER root

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/iceberg-*.jar

USER spark
