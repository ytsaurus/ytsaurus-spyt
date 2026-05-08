package tech.ytsaurus.spark.metrics

import org.slf4j.{Logger, LoggerFactory}
import SolomonConfig.Encoding
import org.apache.spark.SparkEnv
import tech.ytsaurus.spyt.wrapper.config.PropertiesConf
import tech.ytsaurus.spyt.HostAndPort

import java.util.Properties

case class SolomonConfig(
  url: String,
  encoding: Encoding,
  commonLabels: Map[String, String],
  token: Option[String],
  metricNameRegex: String,
  metricNameTransform: Option[String]
)

object SolomonConfig {
  private val log: Logger = LoggerFactory.getLogger(SolomonConfig.getClass)

  sealed trait Encoding {
    def contentType: String
  }

  case object JsonEncoding extends Encoding {
    override def contentType: String = "application/json"
  }

  case object SpackEncoding extends Encoding {
    override def contentType: String = "application/x-solomon-spack"
  }

  def read(props: Properties): SolomonConfig = {

    import SolomonSinkSettings._

    val host = props.ytConf(SolomonHost)
    val port = props.ytConf(SolomonPort)
    val token = props.getYtConf(SolomonToken)
    val commonLabels = buildSolomonCommonLabels(props)
    val metricNameRegex = props.ytConf(SolomonMetricNameRegex)
    val metricNameTransform = Some(props.ytConf(SolomonMetricNameTransform)).filter(!_.isBlank)
    //noinspection UnstableApiUsage
    val hostAndPort = HostAndPort(host, port)
    log.info(s"Solomon host: $host, port: $port commonLabels=$commonLabels")
    SolomonConfig(
      url = s"http://$hostAndPort",
      encoding = JsonEncoding,
      commonLabels,
      token,
      metricNameRegex,
      metricNameTransform
    )
  }

  private def buildSolomonCommonLabels(props: Properties): Map[String, String] = {
    import SolomonSinkSettings._

    if(props.ytConf(SolomonJobLabel).isBlank) {
      props.ytConf(SolomonCommonLabels)
    } else {
      props.ytConf(SolomonCommonLabels) ++
        Map(SolomonJobLabel.name -> props.ytConf(SolomonJobLabel))
    }
  }
}
