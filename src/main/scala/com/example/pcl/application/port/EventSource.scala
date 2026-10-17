package com.example.pcl.application.port

import com.example.pcl.domain.model.CdcEvent

/** Inbound (driving) port: where the application obtains normalized events. Implemented on the
  * "driven" side by the Kafka adapter; substituted by an in-memory source in tests (Liskov
  * substitution across adapters).
  */
trait EventSource {
  def readBatch(maxEvents: Int): Seq[CdcEvent]
}
