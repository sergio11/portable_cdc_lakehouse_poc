package com.example.pcl.application.port

/** Outbound port for rejected events and their machine-readable reasons. */
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
