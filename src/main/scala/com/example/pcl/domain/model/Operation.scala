package com.example.pcl.domain.model

/** Kind of change captured from the source database. */
sealed trait Operation
object Operation {
  case object Create       extends Operation
  case object Update       extends Operation
  case object Delete       extends Operation
  case object SnapshotRead extends Operation

  /** Total set of recognised operations; unknown values are rejected upstream. */
  val all: Set[Operation] = Set(Create, Update, Delete, SnapshotRead)
}
