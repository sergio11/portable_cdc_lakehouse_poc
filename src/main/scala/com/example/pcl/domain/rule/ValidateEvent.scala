package com.example.pcl.domain.rule

import com.example.pcl.domain.model._

/** Pure validation of canonical events. */
object ValidateEvent {

  /** Highest canonical contract version supported by this build. */
  val SupportedSchemaVersion = 1

  def validate(event: CdcEvent): Either[Rejection, CdcEvent] =
    for {
      _ <- checkSchemaVersion(event)
      _ <- checkSourceIdentity(event)
      _ <- checkPosition(event)
      _ <- checkEventId(event)
      _ <- checkPayload(event)
    } yield event

  /** Validate a whole batch, preserving order and collecting every rejection. */
  def validateAll(events: Seq[CdcEvent]): (Seq[CdcEvent], Seq[Rejection]) =
    events.foldLeft((Vector.empty[CdcEvent], Vector.empty[Rejection])) {
      case ((accepted, rejected), event) =>
        validate(event) match {
          case Right(valid) => (accepted :+ valid, rejected)
          case Left(bad)    => (accepted, rejected :+ bad)
        }
    }

  private def checkSchemaVersion(event: CdcEvent): Either[Rejection, Unit] =
    if (event.schemaVersion == SupportedSchemaVersion) Right(())
    else
      Left(
        Rejection(
          ReasonCode.UnsupportedSchemaVersion,
          s"schema_version=${event.schemaVersion}, supported=$SupportedSchemaVersion",
          Some(event.eventId)
        )
      )

  private def checkSourceIdentity(event: CdcEvent): Either[Rejection, Unit] =
    if (event.sourceId.nonEmpty && event.tableName.nonEmpty) Right(())
    else
      Left(
        Rejection(
          ReasonCode.MissingSourceIdentity,
          s"sourceId='${event.sourceId}' tableName='${event.tableName}'",
          Some(event.eventId)
        )
      )

  /** A zero LSN is PostgreSQL's "position unknown" marker, so it is treated as a missing position
    * rather than as the very first change in the stream.
    */
  private def checkPosition(event: CdcEvent): Either[Rejection, Unit] =
    if (event.sourcePosition.lsn > 0L) Right(())
    else
      Left(
        Rejection(
          ReasonCode.MissingSourcePosition,
          s"lsn=${event.sourcePosition.lsn}",
          Some(event.eventId)
        )
      )

  private def checkEventId(event: CdcEvent): Either[Rejection, Unit] =
    if (event.eventId.nonEmpty) Right(())
    else
      Left(
        Rejection(ReasonCode.MissingEnvelopeFields, "event_id is empty")
      )

  /** Require a before image for deletes and an after image for other operations. */
  private def checkPayload(event: CdcEvent): Either[Rejection, Unit] = {
    val hasBefore = event.before.exists(_.nonEmpty)
    val hasAfter  = event.after.exists(_.nonEmpty)

    def bad(detail: String) =
      Left(Rejection(ReasonCode.InconsistentPayload, detail, Some(event.eventId)))

    event.operation match {
      case Operation.Delete =>
        if (hasBefore) Right(()) else bad("DELETE requires a non-empty 'before' image")

      case Operation.Create | Operation.SnapshotRead =>
        if (hasAfter) Right(())
        else bad(s"${opName(event.operation)} requires a non-empty 'after' image")

      case Operation.Update =>
        if (hasAfter) Right(()) else bad("UPDATE requires a non-empty 'after' image")
    }
  }

  private def opName(op: Operation): String = op match {
    case Operation.Create       => "CREATE"
    case Operation.Update       => "UPDATE"
    case Operation.Delete       => "DELETE"
    case Operation.SnapshotRead => "SNAPSHOT_READ"
  }
}
