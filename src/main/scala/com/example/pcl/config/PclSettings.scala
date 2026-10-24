package com.example.pcl.config

/** Typed runtime settings, loaded from the environment and passed explicitly to adapters. */
final case class PclSettings(
    runId: String,
    sourceId: String,
    kafkaBootstrapServers: String = "kafka:9092",
    cdcTopic: String = "pcl.public.orders",
    icebergCatalogUri: String = "http://iceberg-catalog:8181",
    icebergWarehouse: String = "s3://pcl-warehouse/warehouse/",
    s3Endpoint: String = "http://minio:9000",
    s3Region: String = "us-east-1",
    s3AccessKey: String = "",
    s3SecretKey: String = "",
    writeFrequencySeconds: Long = 10L,
    maxRecords: Long = 0L
)

object PclSettings {

  /** Reason codes for configuration problems, mirroring the dead-letter vocabulary. */
  val MissingRunId    = "CONFIG_MISSING_RUN_ID"
  val MissingSourceId = "CONFIG_MISSING_SOURCE_ID"

  /** Build settings from the process environment. Returns a Left with a reason code rather than
    * throwing, so callers decide how to surface misconfiguration.
    */
  def fromEnv(env: Map[String, String]): Either[String, PclSettings] =
    for {
      runId    <- env.get("PCL_RUN_ID").toRight(MissingRunId)
      sourceId <- env.get("PCL_SOURCE_ID").toRight(MissingSourceId)
    } yield PclSettings(
      runId = runId,
      sourceId = sourceId,
      kafkaBootstrapServers = env.getOrElse("PCL_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
      cdcTopic = env.getOrElse("PCL_CDC_TOPIC", "pcl.public.orders"),
      icebergCatalogUri = env.getOrElse("PCL_ICEBERG_CATALOG_URI", "http://iceberg-catalog:8181"),
      icebergWarehouse = env.getOrElse("PCL_ICEBERG_WAREHOUSE", "s3://pcl-warehouse/warehouse/"),
      s3Endpoint = env.getOrElse("PCL_MINIO_ENDPOINT", "http://minio:9000"),
      s3Region = env.getOrElse("PCL_MINIO_REGION", "us-east-1"),
      s3AccessKey = env.getOrElse("PCL_MINIO_ROOT_USER", ""),
      s3SecretKey = env.getOrElse("PCL_MINIO_ROOT_PASSWORD", ""),
      writeFrequencySeconds =
        env.get("PCL_WRITE_FREQUENCY_SECONDS").flatMap(_.toLongOption).getOrElse(10L),
      maxRecords = env.get("PCL_MAX_RECORDS").flatMap(_.toLongOption).getOrElse(0L)
    )
}
