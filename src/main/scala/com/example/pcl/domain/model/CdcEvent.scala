package com.example.pcl.domain.model

/** Canonical CDC event, independent of Debezium's wire format (SRS FR-002).
  *
  * This is the contract every accepted change must satisfy before it reaches the append-only
  * history or the state projection. Field semantics follow SRS 5.1.
  *
  * @param sourceId
  *   stable identifier of the capture source
  * @param schemaName
  *   source logical schema
  * @param tableName
  *   source logical table
  * @param primaryKey
  *   stable entity identity (mandatory)
  * @param operation
  *   the kind of change
  * @param sourcePosition
  *   total-orderable position metadata
  * @param sourceTimestamp
  *   source-provided timestamp, informative only
  * @param transactionId
  *   transaction identifier when available
  * @param eventId
  *   deterministic identity derived from source + position + ordinal
  * @param schemaVersion
  *   version of this canonical contract
  * @param before
  *   prior image, nullable depending on operation
  * @param after
  *   resulting image, nullable depending on operation
  * @param isSnapshot
  *   true when produced by an initial snapshot rather than the WAL
  * @param rawReference
  *   optional pointer back to the preserved raw payload
  */
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
    rawReference: Option[String]
)
