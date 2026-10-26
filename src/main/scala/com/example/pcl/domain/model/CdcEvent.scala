package com.example.pcl.domain.model

/** Canonical CDC event, independent of Debezium's wire format. */
final case class CdcEvent(
    sourceId: String,
    schemaName: String,
    tableName: String,
    primaryKey: EntityKey,
    operation: Operation,
    sourcePosition: SourcePosition,
    sourceTimestamp: Long,
    transactionId: Option[String],
    eventId: String,
    schemaVersion: Int,
    before: Option[Map[String, String]],
    after: Option[Map[String, String]],
    isSnapshot: Boolean,
    rawReference: Option[String],
    /** Wall-clock time at which the connector processed the change. Kept separately from
      * `sourceTimestamp` (the database commit time) so latency can be measured without losing the
      * audit timestamp.
      */
    connectorTimestamp: Long = 0L
)
