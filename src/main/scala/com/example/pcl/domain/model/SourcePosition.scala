package com.example.pcl.domain.model

/** Source-side ordering metadata. A bare LSN is not always unique, so we carry an extra tiebreaker
  * to build a total order per entity (see SRS 5.2).
  *
  * @param lsn
  *   PostgreSQL log sequence number of the change
  * @param txId
  *   transaction id when available, used as a tiebreaker
  * @param sequenceNo
  *   intra-transaction ordinal when available
  */
final case class SourcePosition(lsn: Long, txId: Option[String], sequenceNo: Option[Long]) {

  /** Orders two positions of the same source.
    *
    * LSN decides first. When LSNs are equal we use the available tiebreakers (transaction id, then
    * intra-transaction ordinal) to refine the order. If every tiebreaker that is present on both
    * sides compares equal — or none is available — the two positions are treated as the *same*
    * position (Some(0)); ordering them further is impossible and callers fall back to the
    * deterministic `eventId` to tell redelivery from conflict. This is why the return type is a
    * plain `Int` comparison rather than an optional.
    */
  def comparePosition(other: SourcePosition): Int =
    if (lsn != other.lsn) java.lang.Long.compare(lsn, other.lsn)
    else
      (txId, other.txId) match {
        case (Some(a), Some(b)) if a != b => a.compareTo(b)
        case _ =>
          (sequenceNo, other.sequenceNo) match {
            case (Some(a), Some(b)) => java.lang.Long.compare(a, b)
            case _                  => 0
          }
      }
}
