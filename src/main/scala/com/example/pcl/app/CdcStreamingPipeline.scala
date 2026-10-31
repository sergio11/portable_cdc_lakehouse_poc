package com.example.pcl.app

import com.example.pcl.adapters.debezium.NormalizeCdc
import com.example.pcl.config.PclSettings
import com.example.pcl.domain.model.{CdcEvent, Rejection}
import com.example.pcl.domain.rule.EventIdentity
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.iceberg.{IcebergCatalogConfig, IcebergIO}
import org.apache.beam.sdk.io.kafka.{KafkaIO, KafkaRecord}
import org.apache.beam.sdk.metrics.{Metrics, MetricsFilter}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{PCollectionTuple, Row, TupleTag, TupleTagList}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.iceberg.catalog.TableIdentifier

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.jdk.CollectionConverters._

/** Shared Beam graph for reading Kafka CDC, validating events, and landing accepted rows in Iceberg
  * or routing invalid ones to the dead-letter table.
  */
object CdcStreamingPipeline {

  private val RawTag          = new TupleTag[Row]() {}
  private val DeadLetterTag   = new TupleTag[Row]() {}
  private val AcceptedEvents  = Metrics.counter("pcl.cdc", "accepted_events")
  private val RejectedEvents  = Metrics.counter("pcl.cdc", "rejected_events")
  private val KafkaTombstones = Metrics.counter("pcl.cdc", "kafka_tombstones")

  private val ImageType = Schema.FieldType
    .map(Schema.FieldType.STRING, Schema.FieldType.STRING)
    .withNullable(true)
  private val IcebergTimestampType = Schema.FieldType.logicalType(SqlTypes.DATETIME)

  private val RawSchema: Schema = Schema
    .builder()
    .addStringField("event_id")
    .addStringField("source_id")
    .addStringField("schema_name")
    .addStringField("table_name")
    .addStringField("primary_key")
    .addStringField("operation")
    .addInt64Field("lsn")
    .addNullableField("tx_id", Schema.FieldType.STRING)
    .addNullableField("seq_no", Schema.FieldType.INT64)
    .addField("source_timestamp", IcebergTimestampType.withNullable(true))
    .addField("connector_timestamp", IcebergTimestampType.withNullable(true))
    .addBooleanField("is_snapshot")
    .addField("before_image", ImageType)
    .addField("after_image", ImageType)
    .addInt32Field("schema_version")
    .addNullableField("raw_reference", Schema.FieldType.STRING)
    .addStringField("batch_id")
    .addField("ingested_at", IcebergTimestampType)
    .build()

  private val DeadLetterSchema: Schema = Schema
    .builder()
    .addNullableField("event_id", Schema.FieldType.STRING)
    .addStringField("reason_code")
    .addNullableField("raw_reference", Schema.FieldType.STRING)
    .addNullableField("payload", Schema.FieldType.STRING)
    .addField("observed_at", IcebergTimestampType)
    .addStringField("run_id")
    .build()

  def main(args: Array[String]): Unit =
    PclSettings.fromEnv(sys.env) match {
      case Left(reason) =>
        System.err.println(s"[$reason] Invalid PCL configuration; aborting startup.")
        sys.exit(1)

      case Right(settings) if settings.s3AccessKey.isEmpty || settings.s3SecretKey.isEmpty =>
        System.err.println("[CONFIG_MISSING_S3_CREDENTIALS] MinIO credentials are required.")
        sys.exit(1)

      case Right(settings) =>
        val runnerArg    = s"--runner=${classOf[DirectRunner].getName}"
        val pipelineArgs = if (args.exists(_.startsWith("--runner="))) args else args :+ runnerArg
        val options = PipelineOptionsFactory
          .fromArgs(pipelineArgs: _*)
          .withValidation()
          .as(classOf[PipelineOptions])

        val pipeline = build(options, settings)
        println(
          s"""{"component":"CdcStreamingPipeline","run_id":"${settings.runId}","layer":"${settings.pipelineLayer}","target":"${settings.targetTable}","source_id":"${settings.sourceId}","topic":"${settings.cdcTopic}","status":"running"}"""
        )
        val result = pipeline.run()
        result.waitUntilFinish()
        try {
          val counters =
            result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters.asScala
          counters.foreach { counter =>
            val name = counter.getKey.metricName()
            println(
              s"""{"component":"CdcStreamingPipeline","run_id":"${settings.runId}","metric":"${name.getName}","namespace":"${name.getNamespace}","value":${counter.getAttempted}}"""
            )
          }
        } catch {
          case error: UnsupportedOperationException =>
            System.err.println(s"[METRICS_UNAVAILABLE] ${error.getMessage}")
        }
    }

  private[app] def build(options: PipelineOptions, settings: PclSettings): Pipeline = {
    val pipeline = Pipeline.create(options)
    val catalog  = icebergCatalog(settings)

    val kafkaRead = KafkaIO
      .read[String, String]()
      .withBootstrapServers(settings.kafkaBootstrapServers)
      .withTopic(settings.cdcTopic)
      .withKeyDeserializer(classOf[StringDeserializer])
      .withValueDeserializer(classOf[StringDeserializer])
      .withConsumerConfigUpdates(
        java.util.Map.of[String, Object](
          "group.id",
          settings.consumerGroup,
          "auto.offset.reset",
          "earliest",
          "enable.auto.commit",
          "false"
        )
      )
    val limitedRead =
      if (settings.maxRecords > 0)
        kafkaRead
          .withMaxNumRecords(settings.maxRecords)
          .withMaxReadTime(org.joda.time.Duration.standardSeconds(60))
      else kafkaRead
    // The speed layer runs unbounded on Flink and owns its offsets: KafkaIO commits the
    // checkpointed positions to the speed consumer group after the Iceberg append. The batch
    // layer keeps offsets uncommitted here and lets Dagster commit them after the canonical merge.
    val positionedRead =
      if (settings.commitOffsets) limitedRead.commitOffsetsInFinalize() else limitedRead

    // Keep Kafka metadata: it becomes the raw reference (topic:partition:offset), which is the
    // only stable pointer back to the original record when replaying or investigating a row.
    val messages = pipeline.apply("Read Debezium topic", positionedRead)

    val routed: PCollectionTuple = messages.apply(
      "Normalize and validate Debezium envelopes",
      ParDo
        .of(new NormalizeToRows(settings.sourceId, settings.runId, settings.primaryKeyColumns))
        .withOutputTags(RawTag, TupleTagList.of(DeadLetterTag))
    )

    val landingWriter = IcebergIO
      .writeRows(catalog)
      .to(tableIdentifier(settings.targetTable))
    val deadLetterWriter = IcebergIO
      .writeRows(catalog)
      .to(TableIdentifier.of("ops", "cdc_dead_letter"))
    val configuredLandingWriter =
      if (settings.maxRecords > 0) landingWriter
      else
        landingWriter.withTriggeringFrequency(
          org.joda.time.Duration.standardSeconds(settings.writeFrequencySeconds)
        )
    val configuredDeadLetterWriter =
      if (settings.maxRecords > 0) deadLetterWriter
      else
        deadLetterWriter.withTriggeringFrequency(
          org.joda.time.Duration.standardSeconds(settings.writeFrequencySeconds)
        )

    routed
      .get(RawTag)
      .setRowSchema(RawSchema)
      .apply(s"Append ${settings.pipelineLayer} deliveries", configuredLandingWriter)
    routed
      .get(DeadLetterTag)
      .setRowSchema(DeadLetterSchema)
      .apply("Append rejected envelopes", configuredDeadLetterWriter)

    pipeline
  }

  /** Split `namespace.table` into the identifier Iceberg expects. The settings validate that at
    * least one dot exists, so the index is always positive here.
    */
  private def tableIdentifier(qualified: String): TableIdentifier = {
    val lastDot = qualified.lastIndexOf('.')
    TableIdentifier.of(qualified.substring(0, lastDot), qualified.substring(lastDot + 1))
  }

  private def icebergCatalog(settings: PclSettings): IcebergCatalogConfig = {
    val catalogProperties = new java.util.HashMap[String, String]()
    catalogProperties.put("type", "rest")
    catalogProperties.put("uri", settings.icebergCatalogUri)
    catalogProperties.put("warehouse", settings.icebergWarehouse)
    // Polaris authenticates every catalog call with OAuth2 client credentials.
    // The token endpoint lives under the catalog base URI, and the scope is the
    // Polaris principal role that owns the catalog.
    catalogProperties.put("rest.auth.type", "oauth2")
    catalogProperties.put(
      "credential",
      s"${settings.polarisClientId}:${settings.polarisClientSecret}"
    )
    catalogProperties.put(
      "oauth2-server-uri",
      s"${settings.icebergCatalogUri.stripSuffix("/")}/v1/oauth/tokens"
    )
    catalogProperties.put("scope", "PRINCIPAL_ROLE:ALL")
    catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    catalogProperties.put("s3.endpoint", settings.s3Endpoint)
    catalogProperties.put("s3.path-style-access", "true")
    catalogProperties.put("client.region", settings.s3Region)
    catalogProperties.put("s3.access-key-id", settings.s3AccessKey)
    catalogProperties.put("s3.secret-access-key", settings.s3SecretKey)

    IcebergCatalogConfig
      .builder()
      .setCatalogName("pcl")
      .setCatalogProperties(catalogProperties)
      .setConfigProperties(new java.util.HashMap[String, String]())
      .build()
  }

  private final class NormalizeToRows(
      sourceId: String,
      runId: String,
      primaryKeyColumns: List[String]
  ) extends DoFn[KafkaRecord[String, String], Row] {

    @ProcessElement
    def processElement(context: ProcessContext): Unit = {
      val record       = context.element()
      val payload      = record.getKV.getValue
      val rawReference = s"${record.getTopic}:${record.getPartition}:${record.getOffset}"

      // Debezium emits a Kafka tombstone after a delete envelope. It has no CDC meaning and is
      // skipped; the delete envelope itself was already routed through NormalizeCdc.
      if (payload != null)
        NormalizeCdc.normalize(payload, sourceId, primaryKeyColumns, Some(rawReference)) match {
          case Right(event) =>
            AcceptedEvents.inc()
            context.output(RawTag, toRawRow(event, runId))
          case Left(reason) =>
            RejectedEvents.inc()
            context.output(
              DeadLetterTag,
              toDeadLetterRow(reason.copy(rawReference = Some(rawReference)), payload, runId)
            )
        }
      else KafkaTombstones.inc()
    }
  }

  private def toRawRow(event: CdcEvent, batchId: String): Row = {
    val before = event.before.map(_.asJava).orNull
    val after  = event.after.map(_.asJava).orNull
    def timestamp(millis: Long): LocalDateTime =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)

    // A missing timestamp is represented as null, not as 1970-01-01.
    def optionalTimestamp(millis: Long): LocalDateTime =
      if (millis > 0L) timestamp(millis) else null

    val ingestedAt = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)

    Row
      .withSchema(RawSchema)
      .addValues(
        event.eventId,
        event.sourceId,
        event.schemaName,
        event.tableName,
        EventIdentity.primaryKeyRef(event.primaryKey.values),
        operationName(event),
        Long.box(event.sourcePosition.lsn),
        event.transactionId.orNull,
        event.sourcePosition.sequenceNo.map(Long.box).orNull,
        optionalTimestamp(event.sourceTimestamp),
        optionalTimestamp(event.connectorTimestamp),
        Boolean.box(event.isSnapshot),
        before,
        after,
        Int.box(event.schemaVersion),
        event.rawReference.orNull,
        batchId,
        ingestedAt
      )
      .build()
  }

  private def toDeadLetterRow(reason: Rejection, raw: String, runId: String): Row =
    Row
      .withSchema(DeadLetterSchema)
      .addValues(
        reason.eventId.orNull,
        reason.reason.code,
        reason.rawReference.orNull,
        raw,
        LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC),
        runId
      )
      .build()

  private def operationName(event: CdcEvent): String =
    if (event.isSnapshot) "SNAPSHOT_READ"
    else event.operation.toString.toUpperCase(java.util.Locale.ROOT)
}
