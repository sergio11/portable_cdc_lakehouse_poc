package com.example.pcl.domain.rule

import com.example.pcl.domain.model._

/** Outcome of applying a single CDC event to an entity's projected state (SRS 6.2). The caller
  * (materializer) maps each outcome to a sink action; the domain never touches I/O.
  */
sealed trait ApplyOutcome
object ApplyOutcome {

  /** Event produced a new/updated live state. */
  final case class Applied(next: StateEntity) extends ApplyOutcome

  /** Redelivery of an event already reflected in state; ignore it (SRS FR-009). */
  case object Duplicate extends ApplyOutcome

  /** Event is older than applied state; state is left untouched (SRS 6.2). */
  case object Stale extends ApplyOutcome

  /** No reliable order between positions; caller must quarantine (SRS 5.2). */
  case object Ambiguous extends ApplyOutcome

  /** Same position but conflicting payload; surface a discrepancy, never pick arbitrarily. */
  final case class Conflict(existingEventId: String) extends ApplyOutcome
}

/** Pure state-transition rules for a single entity. No I/O, no clock, no randomness — every
  * decision is a function of (current state, incoming event). This is the core the portability
  * claim rests on: identical logic runs under any Beam runner (ADR-002).
  */
object ApplyChanges {

  import ApplyOutcome._

  /** Apply `event` on top of `current` (None when the entity has no prior state).
    *
    * Rules implemented (SRS 6.2):
    *   - newer position -> apply (create/update/delete)
    *   - same position + same eventId -> Duplicate
    *   - older position -> Stale
    *   - same position + different eventId -> Conflict (only if payload differs)
    *   - unordered positions -> Ambiguous
    *   - delete marks isDeleted and keeps a versioned tombstone (SRS FR-008)
    */
  def apply(current: Option[StateEntity], event: CdcEvent): ApplyOutcome =
    current match {
      case None =>
        // First time we see this entity. Snapshots and creates both seed live state.
        event.operation match {
          case Operation.Delete =>
            // A delete with no prior state still records a tombstone so that an older
            // create replayed later cannot resurrect the entity (SRS FR-008).
            Applied(tombstone(event))
          case _ =>
            Applied(liveState(event))
        }

      case Some(existing) =>
        // `eventId` is the deterministic identity of an event, so it is the primary
        // idempotency key (SRS FR-009): an exact redelivery is a Duplicate regardless
        // of how positions compare. We only consult position ordering to decide
        // whether a *different* event supersedes the applied state.
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
                  // Snapshot must not clobber a newer WAL change (SRS FR-010). Because we
                  // only reach here when the snapshot position is strictly newer, apply it.
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
