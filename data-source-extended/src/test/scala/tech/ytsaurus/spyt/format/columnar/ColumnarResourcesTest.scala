package tech.ytsaurus.spyt.format.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Verifies cleanup preserves provider failures and attempts every resource even when failures are shared. */
class ColumnarResourcesTest extends AnyFlatSpec with Matchers {

  /** Wraps a cleanup action so tests can record execution and inject provider close failures. */
  private def resource(action: => Unit): AutoCloseable = new AutoCloseable {

    /** Executes the cleanup action supplied by the test. */
    override def close(): Unit = action
  }

  "using" should "preserve a failure reused by the body and resource cleanup" in {
    val failure = new IllegalStateException("shared failure")
    var closed = false
    val error = intercept[IllegalStateException] {
      ColumnarResources.using(resource {
        closed = true
        throw failure
      }) {
        throw failure
      }
    }
    error should be theSameInstanceAs failure
    error.getSuppressed shouldBe empty
    closed shouldBe true
  }

  it should "retain a distinct cleanup failure on the original body failure" in {
    val failure = new IllegalStateException("body failure")
    val cleanup = new IllegalArgumentException("cleanup failure")
    val error = intercept[IllegalStateException] {
      ColumnarResources.using(resource(throw cleanup)) {
        throw failure
      }
    }
    error should be theSameInstanceAs failure
    error.getSuppressed.toSeq shouldBe Seq(cleanup)
  }

  "closeAll" should "finish cleanup when multiple resources throw the same failure" in {
    val failure = new IllegalStateException("shared failure")
    val laterFailure = new IllegalArgumentException("later failure")
    var closed = false
    val error = intercept[IllegalStateException] {
      ColumnarResources.closeAll(Iterator(
        resource(throw failure),
        resource(throw failure),
        resource(throw laterFailure),
        resource { closed = true }))
    }
    error should be theSameInstanceAs failure
    error.getSuppressed.toSeq shouldBe Seq(laterFailure)
    closed shouldBe true
  }
}
