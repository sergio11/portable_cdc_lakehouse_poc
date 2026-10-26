package com.example.pcl.domain.model

/** Source ordering metadata; transaction and sequence values break LSN ties when available. */
final case class SourcePosition(lsn: Long, txId: Option[String], sequenceNo: Option[Long]) {

  /** Compare LSN, transaction id, then sequence; return zero when available metadata ties.
    *
    * PostgreSQL transaction ids are numeric. Comparing them as strings breaks once the values have
    * different digit counts (for example "999" vs "1000"), so parse them when possible and fall
    * back to a string comparison only for non-numeric ids.
    */
  def comparePosition(other: SourcePosition): Int =
    if (lsn != other.lsn) java.lang.Long.compare(lsn, other.lsn)
    else
      (txId, other.txId) match {
        case (Some(a), Some(b)) if a != b => compareTx(a, b)
        case _ =>
          (sequenceNo, other.sequenceNo) match {
            case (Some(a), Some(b)) => java.lang.Long.compare(a, b)
            case _                  => 0
          }
      }

  private def compareTx(a: String, b: String): Int =
    (a.toLongOption, b.toLongOption) match {
      case (Some(x), Some(y)) => java.lang.Long.compare(x, y)
      case _                  => a.compareTo(b)
    }
}
