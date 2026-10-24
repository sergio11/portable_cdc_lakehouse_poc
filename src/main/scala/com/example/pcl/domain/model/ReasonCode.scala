package com.example.pcl.domain.model

/** Stable rejection codes for `ops.cdc_dead_letter`; details stay in a separate field. */
sealed abstract class ReasonCode(val code: String)

object ReasonCode {

  /** Envelope could not be parsed as JSON at all. */
  case object MalformedJson extends ReasonCode("MALFORMED_JSON")

  /** Envelope parsed, but the required Debezium envelope fields are absent. */
  case object MissingEnvelopeFields extends ReasonCode("MISSING_ENVELOPE_FIELDS")

  /** `op` was not one of the recognised operations. */
  case object UnknownOperation extends ReasonCode("UNKNOWN_OPERATION")

  /** No primary-key value could be extracted from the row image. */
  case object MissingPrimaryKey extends ReasonCode("MISSING_PRIMARY_KEY")

  /** The source did not supply a usable ordering position (LSN). */
  case object MissingSourcePosition extends ReasonCode("MISSING_SOURCE_POSITION")

  /** `source` metadata is absent or lacks a stable database/table identity. */
  case object MissingSourceIdentity extends ReasonCode("MISSING_SOURCE_IDENTITY")

  /** The row image contradicts the operation, e.g. DELETE with no prior image. */
  case object InconsistentPayload extends ReasonCode("INCONSISTENT_PAYLOAD")

  /** The event declares a contract version this build cannot process. */
  case object UnsupportedSchemaVersion extends ReasonCode("UNSUPPORTED_SCHEMA_VERSION")

  /** Ordering could not be established, so precedence must not be invented. */
  case object AmbiguousOrdering extends ReasonCode("AMBIGUOUS_ORDERING")

  /** The same source position arrived carrying conflicting content. */
  case object ConflictingContent extends ReasonCode("CONFLICTING_CONTENT")

  /** Stable, exhaustive list. Order is irrelevant; membership is the contract. */
  val all: Set[ReasonCode] = Set(
    MalformedJson,
    MissingEnvelopeFields,
    UnknownOperation,
    MissingPrimaryKey,
    MissingSourcePosition,
    MissingSourceIdentity,
    InconsistentPayload,
    UnsupportedSchemaVersion,
    AmbiguousOrdering,
    ConflictingContent
  )
}

/** An event the domain refused, carrying why and enough context to diagnose it. */
final case class Rejection(
    reason: ReasonCode,
    detail: String,
    eventId: Option[String] = None,
    rawReference: Option[String] = None
)
