package com.example.pcl.adapters.debezium

import com.example.pcl.domain.model._
import com.example.pcl.domain.rule.ValidateEvent
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Debezium envelope normalization and canonical event validation. */
class NormalizeCdcSpec extends AnyFlatSpec with Matchers {

  private val Source = "pg-local-1"

  private def insert(lsn: Long = 100L, amount: String = "120.50"): String =
    s"""{
       |  "before": null,
       |  "after": { "order_id": 1001, "customer_id": 501, "status": "NEW", "total_amount": "$amount" },
       |  "source": {
       |    "lsn": $lsn, "txId": 900, "sequence": "[\\"24023119\\",\\"24023128\\"]",
       |    "db": "pcl", "schema": "public", "table": "orders", "ts_ms": 1767225600000
       |  },
       |  "op": "c",
       |  "ts_ms": 1767225600123
       |}""".stripMargin

  private def update(lsn: Long = 200L, amount: String = "349.00"): String =
    s"""{
       |  "before": { "order_id": 1002, "customer_id": 502, "status": "PAID", "total_amount": "340.00" },
       |  "after": { "order_id": 1002, "customer_id": 502, "status": "PAID", "total_amount": "$amount" },
       |  "source": {
       |    "lsn": $lsn, "txId": 901, "sequence": "[\\"24023130\\",\\"24023135\\"]",
       |    "db": "pcl", "schema": "public", "table": "orders", "ts_ms": 1767225601000
       |  },
       |  "op": "u",
       |  "ts_ms": 1767225601123
       |}""".stripMargin

  private def delete(lsn: Long = 300L): String =
    s"""{
       |  "before": { "order_id": 1004, "customer_id": 501, "status": "SHIPPED", "total_amount": "210.00" },
       |  "after": null,
       |  "source": {
       |    "lsn": $lsn, "txId": 902, "sequence": "[\\"24023140\\",\\"24023141\\"]",
       |    "db": "pcl", "schema": "public", "table": "orders", "ts_ms": 1767225602000
       |  },
       |  "op": "d",
       |  "ts_ms": 1767225602123
       |}""".stripMargin

  private def snapshotRow(lsn: Long = 10L): String =
    s"""{
       |  "before": null,
       |  "after": { "order_id": 1005, "customer_id": 504, "status": "CANCELLED", "total_amount": "99.99" },
       |  "source": { "lsn": $lsn, "db": "pcl", "schema": "public", "table": "orders", "ts_ms": 1767225603000 },
       |  "op": "r",
       |  "ts_ms": 1767225603123
       |}""".stripMargin

  "NormalizeCdc" should "map an insert to a canonical CREATE" in {
    val Right(event) = NormalizeCdc.normalize(insert(), Source)

    event.operation shouldBe Operation.Create
    event.tableName shouldBe "orders"
    event.primaryKey.values shouldBe Map("order_id" -> "1001")
    event.sourcePosition.lsn shouldBe 100L
    event.transactionId shouldBe Some("900")
    event.schemaVersion shouldBe ValidateEvent.SupportedSchemaVersion
    event.isSnapshot shouldBe false
    event.after.flatMap(_.get("total_amount")) shouldBe Some("120.50")
  }

  it should "map an update to a canonical UPDATE keeping both images" in {
    val Right(event) = NormalizeCdc.normalize(update(), Source)

    event.operation shouldBe Operation.Update
    event.before.flatMap(_.get("total_amount")) shouldBe Some("340.00")
    event.after.flatMap(_.get("total_amount")) shouldBe Some("349.00")
  }

  it should "map a delete to a canonical DELETE reading the key from the before image" in {
    val Right(event) = NormalizeCdc.normalize(delete(), Source)

    event.operation shouldBe Operation.Delete
    event.primaryKey.values shouldBe Map("order_id" -> "1004")
    event.after shouldBe None
    event.before should not be empty
  }

  it should "flag snapshot rows as SNAPSHOT_READ with is_snapshot set" in {
    val Right(event) = NormalizeCdc.normalize(snapshotRow(), Source)

    event.operation shouldBe Operation.SnapshotRead
    event.isSnapshot shouldBe true
  }

  it should "use source.ts_ms as the database time and top-level ts_ms as the connector time" in {
    val Right(event) = NormalizeCdc.normalize(insert(), Source)

    event.sourceTimestamp shouldBe 1767225600000L
    event.connectorTimestamp shouldBe 1767225600123L
  }

  it should "parse the intra-transaction ordinal from the sequence array" in {
    val Right(event) = NormalizeCdc.normalize(insert(), Source)

    event.sourcePosition.sequenceNo shouldBe Some(24023128L)
  }

  it should "carry the raw reference supplied by the Kafka reader" in {
    val Right(event) =
      NormalizeCdc.normalize(insert(), Source, rawReference = Some("pcl.public.orders:0:42"))

    event.rawReference shouldBe Some("pcl.public.orders:0:42")
  }

  it should "honour a configured primary key column" in {
    val renamed = insert().replace("\"order_id\": 1001", "\"id\": 1001")

    NormalizeCdc
      .normalize(renamed, Source, primaryKeyColumns = List("id"))
      .map(_.primaryKey.values) shouldBe Right(Map("id" -> "1001"))
  }

  it should "derive the same event id for the same change" in {
    val Right(first)  = NormalizeCdc.normalize(insert(), Source)
    val Right(second) = NormalizeCdc.normalize(insert(), Source)
    first.eventId shouldBe second.eventId
  }

  it should "derive different event ids for different source positions" in {
    val Right(first)  = NormalizeCdc.normalize(insert(lsn = 100L), Source)
    val Right(second) = NormalizeCdc.normalize(insert(lsn = 101L), Source)
    first.eventId should not be second.eventId
  }

  it should "keep snapshot rows distinct when they share one source position" in {
    val firstJson     = snapshotRow()
    val secondJson    = snapshotRow().replace("1005", "1006")
    val Right(first)  = NormalizeCdc.normalize(firstJson, Source)
    val Right(second) = NormalizeCdc.normalize(secondJson, Source)

    first.sourcePosition shouldBe second.sourcePosition
    first.eventId should not be second.eventId
  }

  it should "reject a payload that is not JSON" in {
    NormalizeCdc.normalize("{not json", Source) match {
      case Left(rejection) => rejection.reason shouldBe ReasonCode.MalformedJson
      case Right(event)    => fail(s"expected rejection, got $event")
    }
  }

  it should "reject an unknown operation code" in {
    val bad = insert().replace("\"op\": \"c\"", "\"op\": \"z\"")
    NormalizeCdc.normalize(bad, Source) match {
      case Left(rejection) => rejection.reason shouldBe ReasonCode.UnknownOperation
      case Right(event)    => fail(s"expected rejection, got $event")
    }
  }

  it should "reject an event with no usable source position" in {
    val noLsn = insert().replace("\"lsn\": 100", "\"lsn_proc\": 100")
    NormalizeCdc.normalize(noLsn, Source) match {
      case Left(rejection) => rejection.reason shouldBe ReasonCode.MissingSourcePosition
      case Right(event)    => fail(s"expected rejection, got $event")
    }
  }

  it should "reject a row image without the primary key" in {
    val noKey = insert().replace("\"order_id\": 1001, ", "")
    NormalizeCdc.normalize(noKey, Source) match {
      case Left(rejection) => rejection.reason shouldBe ReasonCode.MissingPrimaryKey
      case Right(event)    => fail(s"expected rejection, got $event")
    }
  }

  it should "reject a delete that carries no prior image (tombstone record)" in {
    val tombstone =
      """{"before": null, "after": null,
        | "source": {"lsn": 400, "db": "pcl", "schema": "public", "table": "orders"},
        | "op": "d", "ts_ms": 1767225604000}""".stripMargin
    NormalizeCdc.normalize(tombstone, Source) match {
      case Left(rejection) => rejection.reason shouldBe ReasonCode.InconsistentPayload
      case Right(event)    => fail(s"expected rejection, got $event")
    }
  }

  it should "reject an event with no source metadata" in {
    val noSource =
      """{
        |  "before": null,
        |  "after": { "order_id": 1001, "customer_id": 501, "status": "NEW", "total_amount": "120.50" },
        |  "source": null,
        |  "op": "c",
        |  "ts_ms": 1767225600123
        |}""".stripMargin
    NormalizeCdc.normalize(noSource, Source) match {
      case Left(rejection) => rejection.reason shouldBe ReasonCode.MissingSourceIdentity
      case Right(event)    => fail(s"expected rejection, got $event")
    }
  }

  it should "only ever emit reason codes from the closed vocabulary" in {
    val samples =
      Seq("{bad", insert().replace("\"op\": \"c\"", "\"op\": \"z\""), delete(), snapshotRow())
    samples.foreach { sample =>
      NormalizeCdc.normalize(sample, Source) match {
        case Left(rejection) => ReasonCode.all should contain(rejection.reason)
        case Right(_)        => succeed
      }
    }
  }
}
