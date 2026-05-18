package tech.ytsaurus.spyt

import tech.ytsaurus.spyt.SparkVersionUtils.greaterThanOrEqual

class SparkAdapterProviderImpl4 extends SparkAdapterProviderImpl3 {
  override def createSparkAdapter(sparkVersion: String): SparkAdapter = sparkVersion match {
    case v if greaterThanOrEqual("4.2.0") => SparkAdapter420Impl
    case v if greaterThanOrEqual("4.1.0") => SparkAdapter410Impl
    case v if greaterThanOrEqual("4.0.0") => SparkAdapter400Impl
    case _ => super.createSparkAdapter(sparkVersion)
  }
}

object SparkAdapter400Impl extends SparkAdapter with SparkAdapter330 with SparkAdapter340 with SparkAdapter350
  with SparkAdapter400
object SparkAdapter410Impl extends SparkAdapter with SparkAdapter330 with SparkAdapter340 with SparkAdapter350
  with SparkAdapter400 with SparkAdapter410
object SparkAdapter420Impl extends SparkAdapter with SparkAdapter330 with SparkAdapter340 with SparkAdapter350
  with SparkAdapter400 with SparkAdapter410 with SparkAdapter420
