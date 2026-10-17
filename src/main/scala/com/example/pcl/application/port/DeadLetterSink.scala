package com.example.pcl.application.port

/** Outbound port: where rejected events land with a machine-readable reason (SRS FR-005).
  *
  * @param eventId
  *   originating event id when known
  * @param reasonCode
  *   stable, documented rejection reason
  * @param rawReference
  *   pointer/preview of the offending payload
  * @param observedAt
  *   wall-clock time the rejection was recorded
  */
final case class RejectedEvent(
    eventId: Option[String],
    reasonCode: String,
    rawReference: String,
    observedAt: Long
)

/** Outbound port implemented by the dead-letter sink adapter. */
trait DeadLetterSink {
  def reject(event: RejectedEvent): Unit
}
