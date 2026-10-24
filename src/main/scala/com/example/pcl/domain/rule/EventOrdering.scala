package com.example.pcl.domain.rule

import com.example.pcl.domain.model.{CdcEvent, SourcePosition}

import scala.math.Ordering

/** Total ordering of change events (SRS 5.2).
  *
  * Precedence is derived exclusively from source ordering metadata, never from arrival time or
  * wall-clock timestamps. The comparison chain is:
  *
  *   1. LSN (the authoritative WAL position)
  *   2. transaction id (when the source supplies one)
  *   3. intra-transaction ordinal
  *   4. event id (final tie-break)
  *
  * Step 4 exists purely to make the order deterministic and reproducible. It is not a claim about
  * business precedence: when two distinct events share a source position and disagree on content,
  * the correct outcome is a reported conflict (see [[ApplyChanges.Conflict]]), never a silent
  * winner. Sorting here keeps replay reproducible; [[ApplyChanges]] remains the authority on
  * whether a conflicting pair may be applied.
  */
object EventOrdering {

  /** Source positions order by LSN, then transaction id, then ordinal. */
  implicit val sourcePositionOrdering: Ordering[SourcePosition] =
    (a: SourcePosition, b: SourcePosition) => a.comparePosition(b)

  /** Sort events into the canonical, replay-stable order. */
  def totalOrder(events: Seq[CdcEvent]): Seq[CdcEvent] =
    events.sortBy(event => (event.sourcePosition, event.eventId))

  /** The event that wins for a given key, i.e. the last one in the order. */
  def latestFor(events: Seq[CdcEvent]): Option[CdcEvent] = totalOrder(events).lastOption
}
