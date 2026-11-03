# Iceberg runtime baked in so the SQL job resolves nothing at runtime.
ARG FLINK_VERSION=2.2.1
ARG FLINK_VARIANT=java21
FROM docker.io/library/flink:${FLINK_VERSION}-${FLINK_VARIANT}

ARG ICEBERG_VERSION=1.12.0
# Flink 2.x ships no Hadoop; Iceberg's catalog still needs the client jars.
ARG HADOOP_VERSION=3.4.1

USER root

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.2/${ICEBERG_VERSION}/iceberg-flink-runtime-2.2-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar /opt/flink/lib/

RUN chmod 644 /opt/flink/lib/iceberg-*.jar /opt/flink/lib/hadoop-client-*.jar

USER flink
