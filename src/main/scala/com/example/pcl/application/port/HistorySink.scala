package com.example.pcl.application.port

import com.example.pcl.domain.model.CdcEvent

/** Outbound port for the append-only landing buffer of accepted events. Implementations must never
  * overwrite prior deliveries: duplicates are absorbed later, when the landing buffer is merged
  * into canonical history.
  */
trait HistorySink {
  def append(events: Seq[CdcEvent], batchId: String): Unit
}
