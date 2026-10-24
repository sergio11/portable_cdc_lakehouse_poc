package com.example.pcl.domain.rule

import com.example.pcl.domain.model.CdcEvent

/** Idempotent redelivery handling (SRS FR-009).
  *
  * Redelivery is expected, not exceptional: Kafka re-delivers on rebalance, and a replayed batch is
  * applied twice by design. Because `event_id` is a pure function of the source identity and
  * position, two records describing the same change always carry the same id, so collapsing on that
  * id is safe and loses nothing.
  *
  * These functions are pure: they decide what a collection *should* contain, not what a consumer
  * should do. Beam applies them per window; cross-batch idempotency is additionally guaranteed by
  * [[ApplyChanges]], which refuses to overwrite state it has already seen.
  */
object Deduplicate {

  /** Keep the first occurrence of each `event_id`, preserving input order, and report how many
    * records were collapsed.
    */
  def collapse(events: Seq[CdcEvent]): (Seq[CdcEvent], Int) = {
    val seen   = scala.collection.mutable.LinkedHashSet.empty[String]
    val unique = events.filter(e => seen.add(e.eventId))
    (unique, events.size - unique.size)
  }

  /** Ids that appear more than once, with their multiplicity. */
  def duplicateIds(events: Seq[CdcEvent]): Map[String, Int] =
    events
      .groupBy(_.eventId)
      .view
      .filter(_._2.size > 1)
      .map { case (id, group) => (id, group.size) }
      .toMap

  /** True when the event has already been observed in this collection. */
  def isRedelivery(event: CdcEvent, alreadySeen: Set[String]): Boolean =
    alreadySeen.contains(event.eventId)
}
