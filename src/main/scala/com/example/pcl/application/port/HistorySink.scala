package com.example.pcl.application.port

import com.example.pcl.domain.model.CdcEvent

/** Outbound port for the append-only event log. Implementations must never overwrite or delete a
  * prior delivery: the log is at-least-once, and readers deduplicate by event id.
  */
trait HistorySink {
  def append(events: Seq[CdcEvent], batchId: String): Unit
}
