package tech.ytsaurus.spark.metrics

import tech.ytsaurus.spyt.wrapper.config.ConfigEntry

import scala.concurrent.duration.{Duration, DurationInt}

case object SolomonSinkSettings {
  import ConfigEntry.implicits._

  val YT_MONITORING_PUSH_PORT_ENV_NAME = "YT_METRICS_SPARK_PUSH_PORT"

  case object SolomonHost extends ConfigEntry[String]("solomon_host", Some("[::1]"))
  case object SolomonPort extends ConfigEntry[Int]("solomon_port",
    Option(System.getenv(YT_MONITORING_PUSH_PORT_ENV_NAME)).map(_.toInt))
  case object SolomonToken extends ConfigEntry[String]("solomon_token", None)
  case object SolomonCommonLabels extends ConfigEntry[Map[String, String]]("common_labels", Some(Map()))
  case object SolomonMetricNameRegex extends ConfigEntry[String]("accept_metrics", Some(".*"))
  case object SolomonMetricNameTransform extends ConfigEntry[String]("rename_metrics", Some(""))

  case object ReporterEnabled extends ConfigEntry[Boolean]("reporter_enabled",
    Some(Option(System.getenv("SPARK_YT_METRICS_ENABLED")).forall(_.toBoolean)))
  case object ReporterPollPeriod extends ConfigEntry[Duration]("poll_period",
    Some(500.milliseconds))
  case object ReporterName extends ConfigEntry[String]("reporter_name", Some("spyt"))
}
