package com.example.pcl.config

/** Typed runtime settings, loaded from the environment and passed explicitly to adapters. */
final case class PclSettings(
    runId: String,
    sourceId: String,
    kafkaBootstrapServers: String = "kafka:9092",
    cdcTopic: String = "pcl.public.orders",
    icebergCatalogUri: String = "http://polaris:8181/api/catalog",
    icebergWarehouse: String = "pcl",
    polarisClientId: String = "root",
    polarisClientSecret: String = "",
    s3Endpoint: String = "http://minio:9000",
    s3Region: String = "us-east-1",
    s3AccessKey: String = "",
    s3SecretKey: String = "",
    writeFrequencySeconds: Long = 10L,
    maxRecords: Long = 0L,
    primaryKeyColumns: List[String] = List("order_id"),
    pipelineLayer: String = "batch",
    consumerGroup: String = "",
    targetTable: String = "raw.order_cdc_events",
    commitOffsets: Boolean = false
)

object PclSettings {

  /** Reason codes for configuration problems, mirroring the dead-letter vocabulary. */
  val MissingRunId       = "CONFIG_MISSING_RUN_ID"
  val MissingSourceId    = "CONFIG_MISSING_SOURCE_ID"
  val InvalidTargetTable = "CONFIG_INVALID_TARGET_TABLE"

  /** Iceberg needs a namespace and a table name; reject a bare table before the pipeline starts. */
  private def validTargetTable(name: String): Either[String, String] = {
    val parts = name.split('.').toList
    if (parts.length >= 2 && parts.forall(_.nonEmpty)) Right(name)
    else Left(InvalidTargetTable)
  }

  /** Build settings from the process environment. Returns a Left with a reason code rather than
    * throwing, so callers decide how to surface misconfiguration.
    */
  def fromEnv(env: Map[String, String]): Either[String, PclSettings] =
    for {
      runId    <- env.get("PCL_RUN_ID").toRight(MissingRunId)
      sourceId <- env.get("PCL_SOURCE_ID").toRight(MissingSourceId)
      target <- validTargetTable(env.getOrElse("PCL_TARGET_TABLE", "raw.order_cdc_events"))
    } yield PclSettings(
      runId = runId,
      sourceId = sourceId,
      kafkaBootstrapServers = env.getOrElse("PCL_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
      cdcTopic = env.getOrElse("PCL_CDC_TOPIC", "pcl.public.orders"),
      icebergCatalogUri =
        env.getOrElse("PCL_ICEBERG_CATALOG_URI", "http://polaris:8181/api/catalog"),
      icebergWarehouse = env.getOrElse("PCL_ICEBERG_WAREHOUSE", "pcl"),
      polarisClientId = env.getOrElse("PCL_POLARIS_CLIENT_ID", "root"),
      polarisClientSecret = env.getOrElse("PCL_POLARIS_CLIENT_SECRET", ""),
      s3Endpoint = env.getOrElse("PCL_MINIO_ENDPOINT", "http://minio:9000"),
      s3Region = env.getOrElse("PCL_MINIO_REGION", "us-east-1"),
      s3AccessKey = env.getOrElse("PCL_MINIO_ROOT_USER", ""),
      s3SecretKey = env.getOrElse("PCL_MINIO_ROOT_PASSWORD", ""),
      writeFrequencySeconds =
        env.get("PCL_WRITE_FREQUENCY_SECONDS").flatMap(_.toLongOption).getOrElse(10L),
      maxRecords = env.get("PCL_MAX_RECORDS").flatMap(_.toLongOption).getOrElse(0L),
      primaryKeyColumns = env
        .get("PCL_PRIMARY_KEY_COLUMNS")
        .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toList)
        .filter(_.nonEmpty)
        .getOrElse(List("order_id")),
      pipelineLayer = env.getOrElse("PCL_PIPELINE_LAYER", "batch"),
      consumerGroup = env.getOrElse("PCL_CONSUMER_GROUP", s"pcl-batch-$sourceId"),
      targetTable = target,
      commitOffsets = env.get("PCL_COMMIT_OFFSETS").exists(_.equalsIgnoreCase("true"))
    )
}
