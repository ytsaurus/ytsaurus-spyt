package tech.ytsaurus.spyt

import tech.ytsaurus.spyt.SparkVersionUtils.greaterThanOrEqual

class SparkAdapterProviderImpl extends SparkAdapterProvider {

  override def createSparkAdapter(sparkVersion: String): SparkAdapter = sparkVersion match {
    case v if greaterThanOrEqual("3.5.0") => SparkAdapter350Impl
    case v if greaterThanOrEqual("3.4.0") => SparkAdapter340Impl
    case v if greaterThanOrEqual("3.3.0") => SparkAdapter330Impl
    case v if greaterThanOrEqual("3.2.2") => SparkAdapter322Impl
    case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVersion")
  }
}

object SparkAdapter322Impl extends SparkAdapter with SparkAdapter322
object SparkAdapter330Impl extends SparkAdapter with SparkAdapter322 with SparkAdapter330
object SparkAdapter340Impl extends SparkAdapter with SparkAdapter322 with SparkAdapter330 with SparkAdapter340
object SparkAdapter350Impl extends SparkAdapter with SparkAdapter322 with SparkAdapter330 with SparkAdapter340 with SparkAdapter350
