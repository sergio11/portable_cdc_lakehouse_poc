package com.example.pcl.adapters.debezium

import com.example.pcl.adapters.debezium.DebeziumEnvelope.DecodeFailure
import com.example.pcl.domain.model._
import com.example.pcl.domain.rule.{EventIdentity, ValidateEvent}

/** Converts Debezium envelopes into canonical events and derives operation, identity, and snapshot
  * fields.
  */
object NormalizeCdc {

  /** Debezium operation codes mapped to the canonical operation vocabulary. */
  private val OperationsByCode: Map[String, Operation] = Map(
    "c" -> Operation.Create,
    "u" -> Operation.Update,
    "d" -> Operation.Delete,
    "r" -> Operation.SnapshotRead
  )

  /** Default identity columns for the sample table. Callers can override them through
    * `PCL_PRIMARY_KEY_COLUMNS`, so the adapter is not tied to `orders`.
    */
  val DefaultPrimaryKeyColumns: List[String] = List("order_id")

  def normalize(
      json: String,
      sourceId: String,
      primaryKeyColumns: List[String] = DefaultPrimaryKeyColumns,
      rawReference: Option[String] = None
  ): Either[Rejection, CdcEvent] =
    DebeziumEnvelope
      .decode(json)
      .left
      .map(failure => toRejection(failure))
      .flatMap(envelope => normalizeEnvelope(envelope, sourceId, primaryKeyColumns, rawReference))

  def normalizeEnvelope(
      envelope: DebeziumEnvelope,
      sourceId: String,
      primaryKeyColumns: List[String] = DefaultPrimaryKeyColumns,
      rawReference: Option[String] = None
  ): Either[Rejection, CdcEvent] =
    operationFor(envelope).flatMap { operation =>
      if (operation == Operation.Delete && envelope.before.isEmpty && envelope.after.isEmpty)
        Left(
          Rejection(
            ReasonCode.InconsistentPayload,
            "delete record carries no row image (tombstone)"
          )
        )
      else
        for {
          key   <- primaryKeyFor(envelope, operation, primaryKeyColumns)
          event <- toEvent(envelope, operation, key, sourceId, rawReference)
          valid <- ValidateEvent.validate(event)
        } yield valid
    }

  private def operationFor(envelope: DebeziumEnvelope): Either[Rejection, Operation] =
    OperationsByCode.get(envelope.op) match {
      case Some(operation) => Right(operation)
      case None =>
        Left(
          Rejection(ReasonCode.UnknownOperation, s"unsupported op code '${envelope.op}'")
        )
    }

  /** The row image to read the key from: the `after` image for creates, updates and snapshot rows,
    * and the `before` image for deletes.
    */
  private def rowImage(
      envelope: DebeziumEnvelope,
      operation: Operation
  ): Option[Map[String, String]] =
    if (operation == Operation.Delete) envelope.before else envelope.after

  private def primaryKeyFor(
      envelope: DebeziumEnvelope,
      operation: Operation,
      primaryKeyColumns: List[String]
  ): Either[Rejection, EntityKey] = {
    val image   = rowImage(envelope, operation)
    val missing = primaryKeyColumns.filterNot(column => image.exists(_.contains(column)))

    // A composite key is only meaningful when every configured column is present.
    // Accepting a subset would silently collapse distinct rows into one identity.
    if (missing.nonEmpty)
      Left(
        Rejection(
          ReasonCode.MissingPrimaryKey,
          s"missing key columns in the row image: ${missing.mkString(",")}"
        )
      )
    else
      Right(
        EntityKey(
          primaryKeyColumns.map(column => column -> image.getOrElse(Map.empty)(column)).toMap
        )
      )
  }

  private def toEvent(
      envelope: DebeziumEnvelope,
      operation: Operation,
      key: EntityKey,
      sourceId: String,
      rawReference: Option[String]
  ): Either[Rejection, CdcEvent] = {
    val schemaName = envelope.source.schema.getOrElse("")
    val tableName  = envelope.source.table.getOrElse("")

    val position = SourcePosition(
      lsn = envelope.source.lsn.getOrElse(0L),
      txId = envelope.source.txId.map(_.toString),
      sequenceNo = envelope.source.sequenceNo
    )

    // `source.ts_ms` is the database change time. The top-level `ts_ms` is when the
    // connector processed the record, which is a different measurement. Keep both.
    val databaseTimestamp  = envelope.source.tsMs.orElse(envelope.tsMs).getOrElse(0L)
    val connectorTimestamp = envelope.tsMs.getOrElse(0L)

    Right(
      CdcEvent(
        sourceId = sourceId,
        schemaName = schemaName,
        tableName = tableName,
        primaryKey = key,
        operation = operation,
        sourcePosition = position,
        sourceTimestamp = databaseTimestamp,
        transactionId = envelope.source.txId.map(_.toString),
        // PostgreSQL snapshot rows share one LSN/transaction position. Include the canonical key
        // so distinct rows at that position do not collapse into one event identity.
        eventId = EventIdentity.derive(
          sourceId,
          schemaName,
          tableName,
          position,
          primaryKey = Some(key.values)
        ),
        schemaVersion = ValidateEvent.SupportedSchemaVersion,
        before = envelope.before,
        after = envelope.after,
        isSnapshot = envelope.op == "r",
        rawReference = rawReference,
        connectorTimestamp = connectorTimestamp
      )
    )
  }

  private def toRejection(failure: DecodeFailure): Rejection = {
    val reason = ReasonCode.all.find(_.code == failure.reason).getOrElse(ReasonCode.MalformedJson)
    Rejection(reason, failure.detail)
  }
}
