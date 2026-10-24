package com.example.pcl.domain

import com.example.pcl.domain.model._
import com.example.pcl.domain.rule.ApplyOutcome
import com.example.pcl.domain.rule.ApplyChanges
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Pure state-transition tests; no runner or external services are required. */
class ApplyChangesSpec extends AnyFlatSpec with Matchers {

  private def pos(lsn: Long): SourcePosition = SourcePosition(lsn, txId = None, sequenceNo = None)

  private def event(
      op: Operation,
      lsn: Long,
      id: String,
      after: Option[Map[String, String]] = Some(Map("amount" -> "100")),
      isSnapshot: Boolean = false
  ): CdcEvent =
    CdcEvent(
      sourceId = "pg-1",
      schemaName = "public",
      tableName = "orders",
      primaryKey = EntityKey(Map("order_id" -> "1")),
      operation = op,
      sourcePosition = pos(lsn),
      sourceTimestamp = 1000L + lsn,
      transactionId = None,
      eventId = id,
      schemaVersion = 1,
      before = None,
      after = after,
      isSnapshot = isSnapshot,
      rawReference = None
    )

  "ApplyChanges" should "seed live state from a first create" in {
    val outcome = ApplyChanges(None, event(Operation.Create, lsn = 1L, id = "e1"))
    outcome shouldBe a[ApplyOutcome.Applied]
    val applied = outcome.asInstanceOf[ApplyOutcome.Applied].next
    applied.isDeleted shouldBe false
    applied.data shouldBe Map("amount" -> "100")
    applied.lastEventId shouldBe "e1"
  }

  it should "apply a newer update over older state" in {
    val first = ApplyChanges(None, event(Operation.Create, 1L, "e1"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    val second = ApplyChanges(
      Some(first),
      event(Operation.Update, 2L, "e2", after = Some(Map("amount" -> "200")))
    )
    val applied = second.asInstanceOf[ApplyOutcome.Applied].next
    applied.data shouldBe Map("amount" -> "200")
    applied.lastEventId shouldBe "e2"
  }

  it should "treat exact redelivery as an idempotent duplicate" in {
    val first = ApplyChanges(None, event(Operation.Create, 1L, "e1"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    ApplyChanges(Some(first), event(Operation.Create, 1L, "e1")) shouldBe ApplyOutcome.Duplicate
  }

  it should "ignore an older event without touching state" in {
    val newer = ApplyChanges(None, event(Operation.Create, 5L, "e5"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    ApplyChanges(Some(newer), event(Operation.Update, 3L, "e3")) shouldBe ApplyOutcome.Stale
  }

  it should "record a versioned tombstone on delete" in {
    val live = ApplyChanges(None, event(Operation.Create, 1L, "e1"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    val deleted = ApplyChanges(Some(live), event(Operation.Delete, 2L, "e2"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    deleted.isDeleted shouldBe true
    deleted.data shouldBe empty
    deleted.lastSourcePosition shouldBe pos(2L)
  }

  it should "not resurrect a deleted entity from an older create" in {
    val live = ApplyChanges(None, event(Operation.Create, 1L, "e1"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    val deleted = ApplyChanges(Some(live), event(Operation.Delete, 5L, "e5"))
      .asInstanceOf[ApplyOutcome.Applied]
      .next
    // Replay an older create after the delete: it is stale, so tombstone survives.
    ApplyChanges(Some(deleted), event(Operation.Create, 1L, "e1")) shouldBe ApplyOutcome.Stale
    deleted.isDeleted shouldBe true
  }

  it should "flag a same-position conflicting payload instead of choosing arbitrarily" in {
    val first = ApplyChanges(
      None,
      event(Operation.Update, 1L, "e1", after = Some(Map("amount" -> "100")))
    ).asInstanceOf[ApplyOutcome.Applied].next
    val outcome = ApplyChanges(
      Some(first),
      event(Operation.Update, 1L, "e2", after = Some(Map("amount" -> "999")))
    )
    outcome shouldBe a[ApplyOutcome.Conflict]
  }

  it should "treat a same-position redelivery with identical content as duplicate" in {
    val first = ApplyChanges(
      None,
      event(Operation.Update, 1L, "e1", after = Some(Map("amount" -> "100")))
    ).asInstanceOf[ApplyOutcome.Applied].next
    // Different eventId but identical payload at the same position -> idempotent.
    val outcome = ApplyChanges(
      Some(first),
      event(Operation.Update, 1L, "e2", after = Some(Map("amount" -> "100")))
    )
    outcome shouldBe ApplyOutcome.Duplicate
  }

  it should "seed a tombstone from a first delete so older creates cannot resurrect" in {
    val outcome = ApplyChanges(None, event(Operation.Delete, 1L, "e1"))
    val applied = outcome.asInstanceOf[ApplyOutcome.Applied].next
    applied.isDeleted shouldBe true
  }
}
