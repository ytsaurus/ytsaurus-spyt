package tech.ytsaurus.spyt.wrapper.client

import java.util.concurrent.{CompletableFuture, Semaphore}

case class YtThrottleConfig(maxConcurrency: Int = 0)

class YtThrottle(config: YtThrottleConfig) {
  private val semaphore: Option[Semaphore] =
    if (config.maxConcurrency > 0) Some(new Semaphore(config.maxConcurrency)) else None

  def gate[T](issue: => CompletableFuture[T]): CompletableFuture[T] = semaphore match {
    case None => issue
    case Some(sem) =>
      sem.acquire()
      val future = try {
        issue
      } catch {
        case e: Throwable =>
          sem.release()
          throw e
      }
      future.whenComplete((_, _) => sem.release())
  }
}

object YtThrottle {
  def apply(config: YtThrottleConfig): YtThrottle = new YtThrottle(config)

  def apply(maxConcurrency: Int): YtThrottle = new YtThrottle(YtThrottleConfig(maxConcurrency))
}
