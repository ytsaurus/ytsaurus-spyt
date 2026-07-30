package tech.ytsaurus.spyt.wrapper.client

import tech.ytsaurus.spyt.wrapper.config.ConfigEntry

import java.time.Duration

/**
 * Retries of requests to a fixed proxy, a job proxy among them.
 *
 * Only the responses of an overloaded proxy are paced by the settings below. A transport failure — a timeout
 * or a broken connection — carries no error code, and the client retries it at once whatever the codes are,
 * which is accepted here.
 *
 * @param enabled        retry requests at all, enabled by default
 * @param maxRetries     how many times a request is retried after its first attempt has failed
 * @param initialBackoff pause before the first retry, doubled after every retry
 * @param maxBackoff     upper bound of the pause
 */
case class YtClientRetryConfiguration(enabled: Boolean,
  maxRetries: Int,
  initialBackoff: Duration,
  maxBackoff: Duration) extends Serializable {

  require(maxRetries >= 0, s"maxRetries must be non-negative, but was $maxRetries")
  require(maxRetries < Int.MaxValue, s"maxRetries must be less than ${Int.MaxValue}")

  require(initialBackoff != null, "initialBackoff must not be null")
  require(maxBackoff != null, "maxBackoff must not be null")

  require(
    !initialBackoff.isNegative,
    s"initialBackoff must be non-negative, but was $initialBackoff"
  )
  require(
    !maxBackoff.isNegative,
    s"maxBackoff must be non-negative, but was $maxBackoff"
  )
  require(
    initialBackoff.compareTo(maxBackoff) <= 0,
    s"initialBackoff ($initialBackoff) must not exceed maxBackoff ($maxBackoff)"
  )

  def attemptLimit: Int = maxRetries + 1
}

object YtClientRetryConfiguration {
  import ConfigEntry.implicits._

  private val prefix = "retry"

  case object Enabled extends ConfigEntry[Boolean](s"$prefix.requests.enabled", Some(true))

  case object MaxRetries extends ConfigEntry[Int](s"$prefix.maxRetries", Some(3))

  case object InitialBackoff extends ConfigEntry[Duration](s"$prefix.initialBackoff", Some(Duration.ofSeconds(3)))

  case object MaxBackoff extends ConfigEntry[Duration](s"$prefix.maxBackoff", Some(Duration.ofSeconds(30)))

  val default: YtClientRetryConfiguration = apply(_ => None)

  def apply(getByName: String => Option[String]): YtClientRetryConfiguration = {
    def read[T](entry: ConfigEntry[T]): T = {
      val rawValue = (entry.name +: entry.aliases)
        .iterator
        .map(getByName)
        .collectFirst { case Some(value) => value }

      entry.get(rawValue).getOrElse {
        throw new IllegalArgumentException(
          s"Configuration '${entry.name}' has neither a configured value nor a default"
        )
      }
    }

    YtClientRetryConfiguration(
      enabled = read(Enabled),
      maxRetries = read(MaxRetries),
      initialBackoff = read(InitialBackoff),
      maxBackoff = read(MaxBackoff)
    )
  }
}
