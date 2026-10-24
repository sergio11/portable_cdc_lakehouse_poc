package com.example.pcl.domain.model

/** Source ordering metadata; transaction and sequence values break LSN ties when available. */
final case class SourcePosition(lsn: Long, txId: Option[String], sequenceNo: Option[Long]) {

  /** Compare LSN, transaction id, then sequence; return zero when available metadata ties. */
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
