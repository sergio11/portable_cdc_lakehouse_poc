package com.example.pcl.domain

import com.example.pcl.domain.model.SourcePosition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Ordering semantics for source positions (SRS 5.2). LSN decides first; transaction id and
  * intra-transaction ordinal refine ties; equal positions compare as 0 so callers fall back to
  * `eventId` for idempotency.
  */
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
