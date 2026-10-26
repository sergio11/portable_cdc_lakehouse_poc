package com.example.pcl.adapters.debezium

import io.circe.{ACursor, Json}

/** Wire model of a Debezium PostgreSQL change event.
  *
  * This is an *adapter* concern: it mirrors Debezium's own JSON shape and exists only to be
  * translated into the canonical contract. It deliberately does not live in `domain`, which must
  * stay free of any wire-format or JSON dependency.
  *
  * The field names match Debezium's envelope: { "before": {...}, "after": {...}, "source": {...},
  * "op": "c", "ts_ms": 1 }
  *
  * `op` codes: `c` create/insert, `u` update, `d` delete, `r` snapshot read. A trailing `ts_ms`
  * marks a tombstone produced after a delete, which carries no payload and must not be treated as a
  * second delete.
  */
final case class DebeziumEnvelope(
    before: Option[Map[String, String]],
    after: Option[Map[String, String]],
    source: DebeziumSource,
    op: String,
    tsMs: Option[Long]
)

/** Source-position metadata carried by every Debezium event.
  *
  * PostgreSQL emits `sequence` as a JSON array serialised into a string, for example
  * `"[\"24023119\",\"24023128\"]"`. The last element is the intra-transaction ordinal that
  * disambiguates changes sharing one LSN. `sequenceNo` exposes that ordinal as a number.
  */
final case class DebeziumSource(
    lsn: Option[Long],
    txId: Option[Long],
    sequence: Option[String],
    db: Option[String],
    schema: Option[String],
    table: Option[String],
    tsMs: Option[Long]
) {

  /** Last numeric element of Debezium's `sequence` array, or `None` when absent/malformed. */
  lazy val sequenceNo: Option[Long] =
    sequence.flatMap { raw =>
      val numbers = "\\d+".r.findAllIn(raw).toVector
      numbers.lastOption.flatMap(_.toLongOption)
    }
}

object DebeziumEnvelope {

  /** Reason a payload could not be turned into an envelope. */
  final case class DecodeFailure(reason: String, detail: String)

  /** Decode a Debezium message body.
    *
    * Returns a [[DecodeFailure]] rather than throwing, so a malformed message becomes a rejected
    * event with a reason code instead of killing the pipeline.
    */
  def decode(json: String): Either[DecodeFailure, DebeziumEnvelope] =
    io.circe.parser
      .parse(json)
      .left
      .map(e => DecodeFailure("MALFORMED_JSON", e.getMessage))
      .flatMap(extract)

  private def extract(json: io.circe.Json): Either[DecodeFailure, DebeziumEnvelope] = {
    val cursor = json.hcursor

    def field(name: String): ACursor = cursor.downField(name)

    cursor.downField("op").as[Option[String]] match {
      case Left(err) => Left(DecodeFailure("MISSING_ENVELOPE_FIELDS", s"op: ${err.getMessage}"))
      case Right(None) =>
        Left(DecodeFailure("MISSING_ENVELOPE_FIELDS", "op field is absent"))
      case Right(Some(op)) =>
        for {
          before <- decodeRow(field("before"))
          after  <- decodeRow(field("after"))
          source <- decodeSource(field("source"))
          tsMs   <- Right(cursor.downField("ts_ms").as[Option[Long]].getOrElse(None))
        } yield DebeziumEnvelope(before, after, source, op, tsMs)
    }
  }

  /** Decode a row image into a flat string map.
    *
    * Values are rendered as strings because the canonical contract carries column values as
    * strings: it keeps one representation for every PostgreSQL type Debezium can emit, and it keeps
    * the domain free of type coercion rules.
    */
  private def decodeRow(field: ACursor): Either[DecodeFailure, Option[Map[String, String]]] =
    if (field.failed) Right(None)
    else
      field.as[Option[Map[String, Json]]] match {
        case Left(err) => Left(DecodeFailure("MISSING_ENVELOPE_FIELDS", s"row: ${err.getMessage}"))
        case Right(None) => Right(None)
        case Right(Some(fields)) =>
          val rendered = fields.flatMap { case (k, v) => render(k, v) }
          Right(Some(rendered))
      }

  /** Render a JSON scalar to its canonical string form; drop container values. */
  private def render(key: String, value: Json): Option[(String, String)] =
    value.asString
      .orElse(value.asNumber.map(_.toString))
      .orElse(value.asBoolean.map(_.toString))
      .map(v => (key, v))

  private def decodeSource(field: ACursor): Either[DecodeFailure, DebeziumSource] =
    if (field.failed)
      Left(DecodeFailure("MISSING_SOURCE_IDENTITY", "source metadata is absent"))
    else
      field.as[Option[Map[String, Json]]] match {
        case Left(err)   => Left(DecodeFailure("MISSING_SOURCE_IDENTITY", err.getMessage))
        case Right(None) => Left(DecodeFailure("MISSING_SOURCE_IDENTITY", "source is null"))
        case Right(Some(fields)) =>
          def long(name: String): Option[Long] =
            fields.get(name).flatMap(_.asNumber).flatMap(_.toLong)
          def text(name: String): Option[String] = fields.get(name).flatMap(_.asString)

          // `sequence` is normally a stringified JSON array. Tolerate a real array as well,
          // because some converter configurations preserve the JSON structure.
          def sequence: Option[String] =
            fields.get("sequence").flatMap { value =>
              value.asString.orElse(
                value.asArray.map(_.flatMap(_.asString).mkString("[", ",", "]"))
              )
            }

          Right(
            DebeziumSource(
              lsn = long("lsn"),
              txId = long("txId"),
              sequence = sequence,
              db = text("db"),
              schema = text("schema"),
              table = text("table"),
              tsMs = long("ts_ms")
            )
          )
      }
}
