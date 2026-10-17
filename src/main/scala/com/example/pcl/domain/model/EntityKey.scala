package com.example.pcl.domain.model

/** Stable identity of a captured row. Always present and validated upstream (SRS FR-004).
  *
  * @param values
  *   ordered map of primary-key column name to its string form
  */
final case class EntityKey(values: Map[String, String]) {
  require(values.nonEmpty, "EntityKey must contain at least one primary-key column")

  /** Deterministic, human-readable identity used for logs, grouping and tombstones. */
  def canonical: String = values.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("|")
}
