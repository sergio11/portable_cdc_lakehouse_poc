package com.example.pcl.app

import com.example.pcl.adapters.debezium.NormalizeCdc
import com.example.pcl.config.PclSettings
import com.example.pcl.domain.model.{CdcEvent, Rejection}
import com.example.pcl.domain.rule.EventIdentity
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.iceberg.{IcebergCatalogConfig, IcebergIO}
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.metrics.{Metrics, MetricsFilter}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{KV, PCollectionTuple, Row, TupleTag, TupleTagList}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.iceberg.catalog.TableIdentifier

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.jdk.CollectionConverters._

/** Beam composition root for the streaming CDC application (SRS 2.1).
  *
  * Phase 2 reads Debezium JSON envelopes from Kafka, normalizes and validates them at the adapter
  * boundary, routes malformed records to the dead-letter table, and appends accepted events to raw
  * Iceberg history. The DirectRunner and FlinkRunner use this same graph.
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
          s"""{"component":"CdcStreamingPipeline","run_id":"${settings.runId}","source_id":"${settings.sourceId}","topic":"${settings.cdcTopic}","status":"running"}"""
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
        Map[String, Object](
          "group.id"           -> s"pcl-${settings.sourceId}",
          "auto.offset.reset"  -> "earliest",
          "enable.auto.commit" -> "false"
        ).asJava
      )
    val limitedRead =
      if (settings.maxRecords > 0)
        kafkaRead
          .withMaxNumRecords(settings.maxRecords)
          .withMaxReadTime(org.joda.time.Duration.standardSeconds(60))
      else kafkaRead
    val messages = pipeline.apply("Read Debezium topic", limitedRead.withoutMetadata())

    val routed: PCollectionTuple = messages.apply(
      "Normalize and validate Debezium envelopes",
      ParDo
        .of(new NormalizeToRows(settings.sourceId, settings.runId))
        .withOutputTags(RawTag, TupleTagList.of(DeadLetterTag))
    )

    val rawWriter = IcebergIO
      .writeRows(catalog)
      .to(TableIdentifier.of("raw", "order_cdc_events"))
    val deadLetterWriter = IcebergIO
      .writeRows(catalog)
      .to(TableIdentifier.of("ops", "cdc_dead_letter"))
    val configuredRawWriter =
      if (settings.maxRecords > 0) rawWriter
      else
        rawWriter.withTriggeringFrequency(
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
      .apply("Append canonical history", configuredRawWriter)
    routed
      .get(DeadLetterTag)
      .setRowSchema(DeadLetterSchema)
      .apply("Append rejected envelopes", configuredDeadLetterWriter)

    pipeline
  }

  private def icebergCatalog(settings: PclSettings): IcebergCatalogConfig = {
    val catalogProperties = Map(
      "type"                 -> "rest",
      "uri"                  -> settings.icebergCatalogUri,
      "warehouse"            -> settings.icebergWarehouse,
      "io-impl"              -> "org.apache.iceberg.aws.s3.S3FileIO",
      "s3.endpoint"          -> settings.s3Endpoint,
      "s3.path-style-access" -> "true",
      "client.region"        -> settings.s3Region,
      "s3.access-key-id"     -> settings.s3AccessKey,
      "s3.secret-access-key" -> settings.s3SecretKey
    )

    IcebergCatalogConfig
      .builder()
      .setCatalogName("pcl")
      .setCatalogProperties(catalogProperties.asJava)
      .setConfigProperties(Map.empty[String, String].asJava)
      .build()
  }

  private final class NormalizeToRows(sourceId: String, runId: String)
      extends DoFn[KV[String, String], Row] {

    @ProcessElement
    def processElement(context: ProcessContext): Unit = {
      val payload = context.element().getValue
      // Debezium emits a Kafka tombstone after a delete envelope. It has no CDC meaning and is
      // skipped; the delete envelope itself was already routed through NormalizeCdc.
      if (payload != null)
        NormalizeCdc.normalize(payload, sourceId) match {
          case Right(event) =>
            AcceptedEvents.inc()
            context.output(RawTag, toRawRow(event, payload, runId))
          case Left(reason) =>
            RejectedEvents.inc()
            context.output(DeadLetterTag, toDeadLetterRow(reason, payload, runId))
        }
      else KafkaTombstones.inc()
    }
  }

  private def toRawRow(event: CdcEvent, raw: String, batchId: String): Row = {
    val before = event.before.map(_.asJava).orNull
    val after  = event.after.map(_.asJava).orNull
    val sourceTimestamp = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(event.sourceTimestamp),
      ZoneOffset.UTC
    )
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
        sourceTimestamp,
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
