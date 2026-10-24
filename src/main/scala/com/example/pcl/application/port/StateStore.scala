package com.example.pcl.application.port

import com.example.pcl.domain.model.{EntityKey, StateEntity}

/** Outbound port for the current-state projection store (`silver.orders_current`).
  * Implementations must honour the versioned upsert/delete semantics decided by the domain, not
  * re-implement them.
  */
trait StateStore {
  def get(key: EntityKey): Option[StateEntity]
  def put(state: StateEntity, batchId: String): Unit
  def allKeys(): Seq[EntityKey]
}
