package com.example.pcl.domain.rule

import com.example.pcl.domain.model.SourcePosition

/** Result of comparing an incoming position against the position already applied to an entity (SRS
  * FR-007, 6.2). A sealed ADT (rather than an `Enumeration`) so callers get real exhaustiveness
  * checking at compile time.
  */
sealed trait Verdict

object Verdict {

  /** Incoming position is strictly newer than applied state. */
  case object Newer extends Verdict

  /** Incoming position is strictly older; it must not overwrite state (SRS 6.2). */
  case object Older extends Verdict

  /** Same position. Callers then compare `eventId` to detect idempotent redelivery. */
  case object Same extends Verdict

  /** Defensive only: reserved for future ordering fields that could not be reconciled. */
  case object Ambiguous extends Verdict
}

/** Compares two source positions to decide whether an incoming event supersedes the state already
  * recorded for an entity (SRS FR-007, 6.2).
  */
object VersionComparison {

  /** Decide how `incoming` relates to `applied` for the same entity. */
  def compare(incoming: SourcePosition, applied: SourcePosition): Verdict =
    incoming.comparePosition(applied) match {
      case c if c > 0 => Verdict.Newer
      case c if c < 0 => Verdict.Older
      case _          => Verdict.Same
    }
}
