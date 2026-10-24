package com.example.pcl.domain

import com.example.pcl.domain.model._
import com.example.pcl.domain.rule._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Rules that define event identity, validation, redelivery and ordering (SRS 5.1, 5.2, FR-004,
  * FR-009, AC-02).
  */
class CdcRulesSpec extends AnyFlatSpec with Matchers {

  private def pos(lsn: Long, tx: Option[String] = None, seq: Option[Long] = None) =
    SourcePosition(lsn, tx, seq)

  private def event(
      id: String,
      lsn: Long = 1L,
      op: Operation = Operation.Create,
      before: Option[Map[String, String]] = None,
      after: Option[Map[String, String]] = Some(Map("amount" -> "1")),
      version: Int = 1
  ): CdcEvent =
    CdcEvent(
      sourceId = "pg-1",
      schemaName = "public",
      tableName = "orders",
      primaryKey = EntityKey(Map("order_id" -> "1")),
      operation = op,
      sourcePosition = pos(lsn),
      sourceTimestamp = 1000L,
      transactionId = None,
      eventId = id,
      schemaVersion = version,
      before = before,
      after = after,
      isSnapshot = false,
      rawReference = None
    )

  // --- EventIdentity ---------------------------------------------------------

  "EventIdentity" should "be a pure function of the source identity and position" in {
    val a = EventIdentity.derive("pg-1", "public", "orders", pos(42L, Some("7"), Some(3L)))
    val b = EventIdentity.derive("pg-1", "public", "orders", pos(42L, Some("7"), Some(3L)))
    a shouldBe b
  }

  it should "change when the position changes" in {
    EventIdentity.derive("pg-1", "public", "orders", pos(42L)) should not be
      EventIdentity.derive("pg-1", "public", "orders", pos(43L))
  }

  it should "include the transaction id only when the source supplied one" in {
    val withTx    = EventIdentity.derive("pg-1", "public", "orders", pos(42L, Some("7")))
    val withoutTx = EventIdentity.derive("pg-1", "public", "orders", pos(42L, None))
    withTx should not be withoutTx
    withTx should include("tx=7")
  }

  // --- ValidateEvent ---------------------------------------------------------

  "ValidateEvent" should "accept a well-formed create" in {
    ValidateEvent.validate(event("e1")) shouldBe a[Right[_, _]]
  }

  it should "reject an unsupported contract version" in {
    val bad = event("e1", version = 99)
    ValidateEvent.validate(bad).left.map(_.reason) shouldBe Left(
      ReasonCode.UnsupportedSchemaVersion
    )
  }

  it should "reject a zero LSN as a missing position" in {
    val bad = event("e1", lsn = 0L)
    ValidateEvent.validate(bad).left.map(_.reason) shouldBe Left(ReasonCode.MissingSourcePosition)
  }

  it should "reject a delete with no prior image" in {
    val bad = event("e1", op = Operation.Delete, before = None, after = None)
    ValidateEvent.validate(bad).left.map(_.reason) shouldBe Left(ReasonCode.InconsistentPayload)
  }

  it should "reject a create with no resulting image" in {
    val bad = event("e1", after = None)
    ValidateEvent.validate(bad).left.map(_.reason) shouldBe Left(ReasonCode.InconsistentPayload)
  }

  it should "reject an event with no event id" in {
    val bad = event("", after = None)
    ValidateEvent.validate(bad).left.map(_.reason) shouldBe Left(ReasonCode.MissingEnvelopeFields)
  }

  it should "separate accepted events from rejections and keep input order" in {
    val (accepted, rejected) = ValidateEvent.validateAll(
      Seq(event("e1"), event("e2", version = 99), event("e3"))
    )
    accepted.map(_.eventId) shouldBe Seq("e1", "e3")
    rejected.map(_.reason) shouldBe Seq(ReasonCode.UnsupportedSchemaVersion)
  }

  // --- Deduplicate -----------------------------------------------------------

  "Deduplicate" should "collapse redelivered events keeping the first occurrence" in {
    val (unique, dropped) = Deduplicate.collapse(Seq(event("e1", lsn = 1L), event("e1", lsn = 1L)))
    unique.map(_.eventId) shouldBe Seq("e1")
    dropped shouldBe 1
  }

  it should "leave distinct events untouched" in {
    val input             = Seq(event("e1", lsn = 1L), event("e2", lsn = 2L))
    val (unique, dropped) = Deduplicate.collapse(input)
    unique shouldBe input
    dropped shouldBe 0
  }

  it should "report which ids were duplicated and how often" in {
    val duplicates = Deduplicate.duplicateIds(
      Seq(
        event("e1", lsn = 1L),
        event("e1", lsn = 1L),
        event("e1", lsn = 1L),
        event("e2", lsn = 2L)
      )
    )
    duplicates shouldBe Map("e1" -> 3)
  }

  it should "be idempotent when applied twice (AC-06)" in {
    val input            = Seq(event("e1", lsn = 1L), event("e2", lsn = 2L))
    val (once, _)        = Deduplicate.collapse(input)
    val (twice, dropped) = Deduplicate.collapse(once ++ once)
    twice shouldBe once
    dropped shouldBe once.size
  }

  // --- EventOrdering ---------------------------------------------------------

  "EventOrdering" should "sort by LSN regardless of input order" in {
    val shuffled = Seq(event("e3", lsn = 30L), event("e1", lsn = 10L), event("e2", lsn = 20L))
    EventOrdering.totalOrder(shuffled).map(_.eventId) shouldBe Seq("e1", "e2", "e3")
  }

  it should "break LSN ties by transaction id" in {
    val a = event("a", lsn = 10L).copy(sourcePosition = pos(10L, Some("5")))
    val b = event("b", lsn = 10L).copy(sourcePosition = pos(10L, Some("6")))
    EventOrdering.totalOrder(Seq(b, a)).map(_.eventId) shouldBe Seq("a", "b")
  }

  it should "produce a stable order for equal positions via the event id tie-break" in {
    val a = event("aaa", lsn = 10L)
    val b = event("bbb", lsn = 10L)
    EventOrdering.totalOrder(Seq(b, a)) shouldBe EventOrdering.totalOrder(Seq(a, b))
  }

  it should "select the last event in order as the winner" in {
    val events = Seq(event("e1", lsn = 10L), event("e2", lsn = 20L), event("e3", lsn = 30L))
    EventOrdering.latestFor(events).map(_.eventId) shouldBe Some("e3")
  }

  it should "return None for an empty history" in {
    EventOrdering.latestFor(Seq.empty) shouldBe None
  }

  // --- ReasonCode ------------------------------------------------------------

  "ReasonCode" should "expose a stable, unique code string per reason" in {
    ReasonCode.all.map(_.code).size shouldBe ReasonCode.all.size
    ReasonCode.all.map(_.code).foreach(_ should fullyMatch regex "[A-Z_]+")
  }
}
