package com.example.pcl.config

/** Typed application settings, loaded once at startup from environment variables. The domain and
  * application layers never read the environment directly (SRS FR-015); every value is threaded in
  * explicitly, which keeps the core testable and portable.
  *
  * Only fields required by Phase 0 are present; later phases extend this with Kafka, Iceberg,
  * catalog and runner settings as they are introduced.
  *
  * @param runId
  *   correlates every log line and metric for a single execution
  * @param sourceId
  *   stable identifier stamped onto events for this source database
  */
final case class PclSettings(runId: String, sourceId: String)

object PclSettings {

  /** Reason codes for configuration problems, mirroring the dead-letter vocabulary. */
  val MissingRunId    = "CONFIG_MISSING_RUN_ID"
  val MissingSourceId = "CONFIG_MISSING_SOURCE_ID"

  /** Build settings from the process environment. Returns a Left with a reason code rather than
    * throwing, so callers decide how to surface misconfiguration.
    */
  def fromEnv(env: Map[String, String]): Either[String, PclSettings] =
    for {
      runId    <- env.get("PCL_RUN_ID").toRight(MissingRunId)
      sourceId <- env.get("PCL_SOURCE_ID").toRight(MissingSourceId)
    } yield PclSettings(runId = runId, sourceId = sourceId)
}
