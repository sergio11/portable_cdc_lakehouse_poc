package com.example.pcl.domain.rule

import com.example.pcl.domain.model.SourcePosition

/** Derives a stable event id from source identity, source position, and available tie-breakers. */
object EventIdentity {

  /** Separator that cannot appear in a PostgreSQL identifier. */
  private val Delim = "#"

  /** Build an event id, including optional ordinal and key tie-breakers. */
  def derive(
      sourceId: String,
      schemaName: String,
      tableName: String,
      position: SourcePosition,
      ordinal: Option[Long] = None,
      primaryKey: Option[Map[String, String]] = None
  ): String = {
    val base = List(sourceId, schemaName, tableName, position.lsn.toString).mkString(Delim)

    val withTx  = position.txId.fold(base)(tx => s"$base${Delim}tx=$tx")
    val withSeq = position.sequenceNo.fold(withTx)(seq => s"$withTx${Delim}seq=$seq")
    val withKey = primaryKey.fold(withSeq)(key => s"$withSeq${Delim}key=${primaryKeyRef(key)}")

    ordinal.fold(withKey)(ord => s"$withKey${Delim}ord=$ord")
  }

  /** Canonical key of a row, used for grouping and for dead-letter context. */
  def primaryKeyRef(key: Map[String, String]): String =
    key.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(Delim)
}
