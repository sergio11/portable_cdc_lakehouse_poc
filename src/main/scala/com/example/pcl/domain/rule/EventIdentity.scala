package com.example.pcl.domain.rule

import com.example.pcl.domain.model.SourcePosition

/** Deterministic identity of a captured change (SRS 5.1, `event_id`).
  *
  * The id must be a pure function of the source identity and the source ordering position, so that
  * the same change always yields the same id regardless of which consumer, which run or which
  * replay produced it. That property is what makes redelivery detectable (SRS FR-009) and what lets
  * a replay be compared against the original stream.
  *
  * The position components that actually identify the change are included; components the source
  * did not supply are skipped rather than defaulted, so two events that differ in a supplied field
  * can never collide.
  */
object EventIdentity {

  /** Separator that cannot appear in a PostgreSQL identifier. */
  private val Delim = "#"

  /** Build the canonical event id for a change.
    *
    * @param sourceId
    *   stable identifier of the capture source
    * @param schemaName
    *   source logical schema
    * @param tableName
    *   source logical table
    * @param position
    *   ordering position; LSN is always present in practice
    * @param ordinal
    *   intra-position ordinal, used when a source emits more than one change for the same position
    *   (e.g. a multi-row transaction sharing an LSN)
    */
  def derive(
      sourceId: String,
      schemaName: String,
      tableName: String,
      position: SourcePosition,
      ordinal: Option[Long] = None
  ): String = {
    val base = List(sourceId, schemaName, tableName, position.lsn.toString).mkString(Delim)

    val withTx  = position.txId.fold(base)(tx => s"$base${Delim}tx=$tx")
    val withSeq = position.sequenceNo.fold(withTx)(seq => s"$withTx${Delim}seq=$seq")

    ordinal.fold(withSeq)(ord => s"$withSeq${Delim}ord=$ord")
  }

  /** Canonical key of a row, used for grouping and for dead-letter context. */
  def primaryKeyRef(key: Map[String, String]): String =
    key.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(Delim)
}
