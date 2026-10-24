package com.example.pcl.domain.rule

import com.example.pcl.domain.model.{CdcEvent, SourcePosition}

import scala.math.Ordering

/** Orders by source position, then event id for deterministic replay. Conflicting content is
  * handled by [[ApplyChanges]], not by this sorting tie-break.
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
