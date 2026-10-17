package com.example.pcl.domain.model

/** Projected current state of a single entity, derived from the append-only history (SRS 6.1,
  * `silver.orders_current`). Immutable; every change produces a new value.
  *
  * @param key
  *   stable entity identity
  * @param data
  *   latest business columns for the entity
  * @param isDeleted
  *   true once a delete tombstone has been applied
  * @param lastSourcePosition
  *   position of the event that produced this state
  * @param lastEventId
  *   id of the event that produced this state (idempotency)
  * @param updatedAt
  *   source timestamp of the applied event
  */
final case class StateEntity(
    key: EntityKey,
    data: Map[String, String],
    isDeleted: Boolean,
    lastSourcePosition: SourcePosition,
    lastEventId: String,
    updatedAt: Long
)
