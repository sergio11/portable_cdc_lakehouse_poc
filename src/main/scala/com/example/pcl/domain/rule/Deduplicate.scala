package com.example.pcl.domain.rule

import com.example.pcl.domain.model.CdcEvent

/** Pure helpers for collapsing repeated event ids within a collection. */
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
