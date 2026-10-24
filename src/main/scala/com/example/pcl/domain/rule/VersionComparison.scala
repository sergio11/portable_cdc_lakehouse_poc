package com.example.pcl.domain.rule

import com.example.pcl.domain.model.SourcePosition

/** Result of comparing incoming and applied source positions. */
sealed trait Verdict

object Verdict {

  /** Incoming position is strictly newer than applied state. */
  case object Newer extends Verdict

  /** Incoming position is older; it must not overwrite state. */
  case object Older extends Verdict

  /** Same position. Callers then compare `eventId` to detect idempotent redelivery. */
  case object Same extends Verdict

  /** Defensive only: reserved for future ordering fields that could not be reconciled. */
  case object Ambiguous extends Verdict
}

/** Compares source positions to determine whether an event supersedes current state. */
object VersionComparison {

  /** Decide how `incoming` relates to `applied` for the same entity. */
  def compare(incoming: SourcePosition, applied: SourcePosition): Verdict =
    incoming.comparePosition(applied) match {
      case c if c > 0 => Verdict.Newer
      case c if c < 0 => Verdict.Older
      case _          => Verdict.Same
    }
}
