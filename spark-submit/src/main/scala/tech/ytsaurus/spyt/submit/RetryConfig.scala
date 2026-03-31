package tech.ytsaurus.spyt.submit

import java.time.Duration


case class RetryConfig(
  enableRetry: Boolean = true,
  retryLimit: Int = 10,
  retryInterval: Duration = Duration.ofMinutes(1),
  waitSubmissionIdRetryLimit: Int = 50) {
  def this(enableRetry: Boolean, retryLimit: Int, retryInterval: Duration) {
    this(enableRetry, retryLimit, retryInterval, 50)
  }
}

object RetryConfig {
  // for Python wrapper
  def durationFromSeconds(amount: Int): Duration = Duration.ofSeconds(amount)
}
