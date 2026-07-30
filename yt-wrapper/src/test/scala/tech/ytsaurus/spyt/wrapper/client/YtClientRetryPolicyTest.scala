package tech.ytsaurus.spyt.wrapper.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.TError
import tech.ytsaurus.client.rpc.RpcOptions
import tech.ytsaurus.core.common.{YTsaurusError, YTsaurusErrorCode}
import tech.ytsaurus.spyt.wrapper.YtWrapper.RichRpcOptions

import java.time.Duration
import java.util.concurrent.TimeoutException

class YtClientRetryPolicyTest extends AnyFlatSpec with Matchers {

  private val overloadCode = YTsaurusErrorCode.RequestQueueSizeLimitExceeded.getCode

  private val retryTwice = YtClientRetryConfiguration(enabled = true, maxRetries = 2,
    initialBackoff = Duration.ofSeconds(3), maxBackoff = Duration.ofSeconds(10))

  private def error(code: Int): YTsaurusError =
    new YTsaurusError(TError.newBuilder().setCode(code).setMessage(s"error with code $code").build())

  private def retries(retry: YtClientRetryConfiguration, failure: Throwable): Seq[Duration] = {
    val options = new RpcOptions().setRetries(retry)
    val policy = options.getRetryPolicyFactory.get()
    val limit = retry.maxRetries + 2
    val pauses = (1 to limit).iterator
      .map { _ =>
        policy.onNewAttempt()
        policy.getBackoffDuration(failure, options)
      }
      .takeWhile(_.isPresent)
      .map(_.get())
      .toSeq

    withClue("the policy kept retrying past any sane limit: ") {
      pauses.size should be < limit
    }
    pauses
  }

  behavior of "SPYT retry policy"

  it should "retry a request rejected by an overloaded proxy" in {
    val pauses = retries(retryTwice, error(overloadCode))

    pauses.size shouldBe retryTwice.maxRetries
    pauses.head shouldBe retryTwice.initialBackoff
    pauses.last should be <= retryTwice.maxBackoff
  }

  it should "not retry an error a retry cannot help with" in {
    retries(retryTwice, error(YTsaurusErrorCode.NoSuchTransaction.getCode)) shouldBe empty
  }

  it should "retry a transport timeout immediately" in {
    val pauses = retries(retryTwice, new TimeoutException("no response"))

    pauses.size shouldBe retryTwice.maxRetries
    pauses.distinct shouldBe Seq(Duration.ZERO)
  }

  it should "not retry when retries are switched off" in {
    val disabled = retryTwice.copy(enabled = false)

    retries(disabled, error(overloadCode)) shouldBe empty
  }
}
