package com.example.pcl.domain

import com.example.pcl.domain.model.SourcePosition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Position ordering uses LSN, then transaction id and sequence to refine ties. */
class SourcePositionSpec extends AnyFlatSpec with Matchers {

  "comparePosition" should "order by LSN when LSNs differ" in {
    val a = SourcePosition(10L, Some("5"), Some(1L))
    val b = SourcePosition(20L, Some("1"), Some(9L))
    a.comparePosition(b) should be < 0
    b.comparePosition(a) should be > 0
  }

  it should "break LSN ties with the transaction id when both sides carry one" in {
    val a = SourcePosition(10L, Some("1"), None)
    val b = SourcePosition(10L, Some("2"), None)
    a.comparePosition(b) should be < 0
  }

  it should "compare numeric transaction ids by value, not by string length" in {
    val older = SourcePosition(10L, Some("999"), None)
    val newer = SourcePosition(10L, Some("1000"), None)

    older.comparePosition(newer) should be < 0
    newer.comparePosition(older) should be > 0
  }

  it should "fall back to string comparison for non-numeric transaction ids" in {
    val a = SourcePosition(10L, Some("tx-a"), None)
    val b = SourcePosition(10L, Some("tx-b"), None)
    a.comparePosition(b) should be < 0
  }

  it should "break remaining ties with the intra-transaction ordinal" in {
    val a = SourcePosition(10L, Some("1"), Some(1L))
    val b = SourcePosition(10L, Some("1"), Some(2L))
    a.comparePosition(b) should be < 0
  }

  it should "treat equal LSN with no tiebreaker as the same position" in {
    val a = SourcePosition(10L, None, None)
    val b = SourcePosition(10L, None, None)
    a.comparePosition(b) shouldBe 0
  }

  it should "treat equal LSN with equal tiebreakers as the same position" in {
    val a = SourcePosition(10L, Some("1"), Some(1L))
    val b = SourcePosition(10L, Some("1"), Some(1L))
    a.comparePosition(b) shouldBe 0
  }

  it should "ignore a tiebreaker present on only one side" in {
    // One side has a txId, the other does not: we cannot order by it, so fall through
    // to the ordinal, which is also absent -> same position.
    val a = SourcePosition(10L, Some("1"), None)
    val b = SourcePosition(10L, None, None)
    a.comparePosition(b) shouldBe 0
  }
}
