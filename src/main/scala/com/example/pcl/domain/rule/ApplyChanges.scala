package com.example.pcl.domain.rule

import com.example.pcl.domain.model._

/** Result of applying one event to projected state. */
sealed trait ApplyOutcome
object ApplyOutcome {

  /** Event produced a new/updated live state. */
  final case class Applied(next: StateEntity) extends ApplyOutcome

  /** Exact redelivery; ignore it. */
  case object Duplicate extends ApplyOutcome

  /** Older event; leave state unchanged. */
  case object Stale extends ApplyOutcome

  /** Position cannot be ordered; caller must quarantine. */
  case object Ambiguous extends ApplyOutcome

  /** Same position but conflicting payload; surface a discrepancy, never pick arbitrarily. */
  final case class Conflict(existingEventId: String) extends ApplyOutcome
}

/** Pure state transitions shared by all Beam runners. */
object ApplyChanges {

  import ApplyOutcome._

  /** Apply an event to the current state, if present. */
  def apply(current: Option[StateEntity], event: CdcEvent): ApplyOutcome =
    current match {
      case None =>
        // First time we see this entity. Snapshots and creates both seed live state.
        event.operation match {
          case Operation.Delete =>
            // Keep a tombstone so an older replay cannot resurrect the entity.
            Applied(tombstone(event))
          case _ =>
            Applied(liveState(event))
        }

      case Some(existing) =>
        // Event identity detects redelivery before comparing source positions.
        if (event.eventId == existing.lastEventId) Duplicate
        else
          VersionComparison.compare(event.sourcePosition, existing.lastSourcePosition) match {
            case Verdict.Ambiguous => Ambiguous

            case Verdict.Older => Stale

            case Verdict.Same =>
              // Distinct event at the same position: conflict only if content differs.
              if (payloadDiffers(existing, event)) Conflict(existing.lastEventId)
              else Duplicate

            case Verdict.Newer =>
              event.operation match {
                case Operation.Delete       => Applied(tombstone(event))
                case Operation.SnapshotRead =>
                  // Apply only when its source position is newer.
                  Applied(liveState(event))
                case _ => Applied(liveState(event))
              }
          }
    }

  private def liveState(event: CdcEvent): StateEntity =
    StateEntity(
      key = event.primaryKey,
      data = event.after.getOrElse(Map.empty),
      isDeleted = false,
      lastSourcePosition = event.sourcePosition,
      lastEventId = event.eventId,
      updatedAt = event.sourceTimestamp
    )

  private def tombstone(event: CdcEvent): StateEntity =
    StateEntity(
      key = event.primaryKey,
      data = Map.empty,
      isDeleted = true,
      lastSourcePosition = event.sourcePosition,
      lastEventId = event.eventId,
      updatedAt = event.sourceTimestamp
    )

  /** Two events at the same position conflict only when their resulting business images differ.
    * Comparing the canonical `after` map keeps this decision content-based.
    */
  private def payloadDiffers(existing: StateEntity, event: CdcEvent): Boolean = {
    val incoming = event.after.getOrElse(Map.empty)
    // For deletes the projected image is empty, so compare against an empty map.
    val expected = if (event.operation == Operation.Delete) Map.empty[String, String] else incoming
    existing.data != expected || existing.isDeleted != (event.operation == Operation.Delete)
  }
}
