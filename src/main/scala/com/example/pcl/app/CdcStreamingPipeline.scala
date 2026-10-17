package com.example.pcl.app

import com.example.pcl.config.PclSettings

/** Composition root for the streaming CDC pipeline (SRS 2.1).
  *
  * Phase 0 proves the hexagonal skeleton compiles and the portability gate runs. The full Kafka ->
  * transform -> Iceberg graph is wired in later phases; this entrypoint already validates
  * configuration so misconfiguration fails fast at startup (FR-015).
  */
object CdcStreamingPipeline {

  def main(args: Array[String]): Unit = {
    val env = sys.env

    PclSettings.fromEnv(env) match {
      case Left(reason) =>
        System.err.println(s"[$reason] Invalid PCL configuration; aborting startup.")
        sys.exit(1)

      case Right(settings) =>
        println(
          s"""{"component":"CdcStreamingPipeline","run_id":"${settings.runId}","source_id":"${settings.sourceId}","phase":0,"status":"skeleton_ready"}"""
        )
    }
  }
}
