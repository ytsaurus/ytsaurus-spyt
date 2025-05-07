package tech.ytsaurus.spyt.wrapper

import java.net.InetAddress
import scala.concurrent.duration._

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

  def tryWithResources[R <: AutoCloseable, A](resource: R)(body: R => A): A = TryWithResourcesJava.apply(resource, body)
}
