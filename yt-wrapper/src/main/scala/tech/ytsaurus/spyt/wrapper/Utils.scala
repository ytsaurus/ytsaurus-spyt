package tech.ytsaurus.spyt.wrapper

import java.io.{FileInputStream, InputStream}
import java.net.InetAddress
import java.util.Properties
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object Utils {
  def parseDuration(s: String): Duration = {
    val regex = """(\d+)(.*)""".r
    s match {
      case regex(amount, "") => amount.toInt.seconds
      case regex(amount, "s") => amount.toInt.seconds
      case regex(amount, "m") => amount.toInt.minutes
      case regex(amount, "min") => amount.toInt.minutes
      case regex(amount, "h") => amount.toInt.hours
      case regex(amount, "d") => amount.toInt.days
      case regex(_, unit) => throw new IllegalArgumentException(s"Unknown time unit: $unit")
      case _ => throw new IllegalArgumentException(s"Illegal time format: $s")
    }
  }

  def flatten[A, B](seq: Seq[Either[A, B]]): Either[A, Seq[B]] = {
    seq
      .find(_.isLeft)
      .map(e => Left(e.left.get))
      .getOrElse(Right(seq.map(_.right.get)))
  }

  def ytNetworkProjectEnabled: Boolean = sys.env.contains("YT_NETWORK_PROJECT_ID")

  def ytHostIp: String = {
    val ipString = sys.env("YT_IP_ADDRESS_DEFAULT")
    if (ipString.contains(":") && !ipString.startsWith("[") && !ipString.endsWith("]")) {
      s"[$ipString]"
    } else {
      ipString
    }
  }

  def ytHostIpBashInlineWrapper(addressEnvVar: String): String =
    """$(if [[ "$%1$s" == *:* ]]; then echo "[$%1$s]"; else echo "$%1$s"; fi)"""
      .format(addressEnvVar)

  def bashCommand(args: String*): String = args.map { arg => s"'${arg.replace("'", "'\"'\"'")}'" }.mkString(" ")

  lazy val sparkSystemProperties: Map[String, String] = {
    import scala.collection.JavaConverters._
    System.getProperties.stringPropertyNames().asScala.collect {
      case name if name.startsWith("spark.") => name -> System.getProperty(name)
    }.toMap
  }

  def ytHostnameOrIpAddress: String =
    if (ytNetworkProjectEnabled)
      ytHostIp
    else if (sparkSystemProperties.get("spark.yt.useFqdn").exists(_.toBoolean))
      InetAddress.getLocalHost.getCanonicalHostName
    else
      InetAddress.getLocalHost.getHostName

  def tryUpdatePropertiesFromFile(path: String, properties: Properties): Unit = {
    var is: InputStream = null
    try {
      is = new FileInputStream(path)
      properties.load(is)
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

  def tryWithResources[R <: AutoCloseable, A](resource: R)(body: R => A): A = TryWithResourcesJava.apply(resource, body)

  def runWithRetry[T](
                       operation: => CompletableFuture[T],
                       maxRetries: Int = 3,
                       initialDelay: FiniteDuration = 5.second,
                       maxDelay: FiniteDuration = 60.seconds,
                       timeout: FiniteDuration = 30.seconds
                     ): T = {
    require(maxRetries >= 0, "maxRetries must be non-negative")

    @tailrec
    def attempt(retry: Int, currentDelay: FiniteDuration): T = {
      val result = Try(operation.get(timeout.toMillis, TimeUnit.MILLISECONDS))
      result match {
        case Success(value) => value
        case Failure(_) if retry > 0 =>
          Thread.sleep(currentDelay.toMillis)
          val nextDelay = (currentDelay * 2).min(maxDelay)
          attempt(retry - 1, nextDelay)
        case Failure(ex) =>
          throw new RuntimeException(s"Operation failed after ${maxRetries + 1} attempts", ex)
      }
    }
    attempt(maxRetries, initialDelay)
  }
}
