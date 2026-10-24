package com.example.pcl.application.port

import com.example.pcl.domain.model.CdcEvent

/** Outbound port for append-only persistence of accepted events. Implementations must
  * never overwrite prior events.
  */
trait HistorySink {
  def append(events: Seq[CdcEvent], batchId: String): Unit
}
