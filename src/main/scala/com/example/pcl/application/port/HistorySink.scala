package com.example.pcl.application.port

import com.example.pcl.domain.model.CdcEvent

/** Outbound port: append-only persistence of accepted events (SRS FR-006). Implementations must
  * never overwrite prior events.
  */
trait HistorySink {
  def append(events: Seq[CdcEvent], batchId: String): Unit
}
