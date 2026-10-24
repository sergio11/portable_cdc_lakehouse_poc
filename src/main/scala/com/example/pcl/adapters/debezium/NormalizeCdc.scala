package com.example.pcl.adapters.debezium

import com.example.pcl.adapters.debezium.DebeziumEnvelope.DecodeFailure
import com.example.pcl.domain.model._
import com.example.pcl.domain.rule.{EventIdentity, ValidateEvent}

/** Translates Debezium envelopes into the canonical CDC contract (SRS FR-002).
  *
  * This is the boundary where wire format stops and the domain begins. Everything downstream of
  * `normalize` deals exclusively with [[CdcEvent]], so a change in Debezium's payload shape is
  * contained to this adapter and the domain rules stay untouched.
  *
  * Normalisation is where the canonical fields are *derived*, in particular:
  *   - `operation` from Debezium's single-letter `op`;
  *   - `event_id` from source identity plus ordering position, so the id is a pure function of the
  *     change and redelivery becomes detectable (SRS FR-009);
  *   - `is_snapshot` from the `r` operation code.
  */
object NormalizeCdc {

  /** Debezium operation codes mapped to the canonical operation vocabulary. */
  private val OperationsByCode: Map[String, Operation] = Map(
    "c" -> Operation.Create,
    "u" -> Operation.Update,
    "d" -> Operation.Delete,
    "r" -> Operation.SnapshotRead
  )

  /** Columns that identify a row. Configured, not hard-coded per table. */
  private val PrimaryKeyColumns = List("order_id")

  def normalize(json: String, sourceId: String): Either[Rejection, CdcEvent] =
    DebeziumEnvelope
      .decode(json)
      .left
      .map(failure => toRejection(failure))
      .flatMap(envelope => normalizeEnvelope(envelope, sourceId))

  def normalizeEnvelope(
      envelope: DebeziumEnvelope,
      sourceId: String
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
          key   <- primaryKeyFor(envelope, operation)
          event <- toEvent(envelope, operation, key, sourceId)
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
      operation: Operation
  ): Either[Rejection, EntityKey] = {
    val values = PrimaryKeyColumns.flatMap { column =>
      rowImage(envelope, operation).flatMap(_.get(column)).map(column -> _)
    }

    if (values.isEmpty)
      Left(
        Rejection(
          ReasonCode.MissingPrimaryKey,
          s"none of ${PrimaryKeyColumns.mkString(",")} present in the row image"
        )
      )
    else Right(EntityKey(values.toMap))
  }

  private def toEvent(
      envelope: DebeziumEnvelope,
      operation: Operation,
      key: EntityKey,
      sourceId: String
  ): Either[Rejection, CdcEvent] = {
    val schemaName = envelope.source.schema.getOrElse("")
    val tableName  = envelope.source.table.getOrElse("")

    val position = SourcePosition(
      lsn = envelope.source.lsn.getOrElse(0L),
      txId = envelope.source.txId.map(_.toString),
      sequenceNo = envelope.source.sequenceNo.flatMap(_.toLongOption)
    )

    Right(
      CdcEvent(
        sourceId = sourceId,
        schemaName = schemaName,
        tableName = tableName,
        primaryKey = key,
        operation = operation,
        sourcePosition = position,
        sourceTimestamp = envelope.tsMs.getOrElse(0L),
        transactionId = envelope.source.txId.map(_.toString),
        eventId = EventIdentity.derive(sourceId, schemaName, tableName, position),
        schemaVersion = ValidateEvent.SupportedSchemaVersion,
        before = envelope.before,
        after = envelope.after,
        isSnapshot = envelope.op == "r",
        rawReference = None
      )
    )
  }

  private def toRejection(failure: DecodeFailure): Rejection = {
    val reason = ReasonCode.all.find(_.code == failure.reason).getOrElse(ReasonCode.MalformedJson)
    Rejection(reason, failure.detail)
  }
}
