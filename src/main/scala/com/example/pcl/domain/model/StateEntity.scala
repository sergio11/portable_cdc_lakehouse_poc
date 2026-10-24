package com.example.pcl.domain.model

/** Immutable projection of one entity, including its last applied position and tombstone state. */
final case class StateEntity(
    key: EntityKey,
    data: Map[String, String],
    isDeleted: Boolean,
    lastSourcePosition: SourcePosition,
    lastEventId: String,
    updatedAt: Long
)
