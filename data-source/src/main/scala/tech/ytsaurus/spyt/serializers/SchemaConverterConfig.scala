package tech.ytsaurus.spyt.serializers

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.SparkVersionUtils
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.YsonAsVariant
import tech.ytsaurus.spyt.wrapper.config.{SparkYtSparkConf, SparkYtSparkSession, sparkConfigKeys}

/** Throws UnsupportedOperationException for ysonAsVariant on Spark versions without the Variant type. */
case class SchemaConverterConfig(parsingTypeV3: Boolean, ysonAsVariant: Boolean) {
  if (ysonAsVariant && SparkVersionUtils.lessThan(SchemaConverterConfig.VARIANT_SPARK_VERSION)) {
    throw new UnsupportedOperationException(s"Spark ${SparkVersionUtils.currentVersion} has no Variant type, so " +
      s"${sparkConfigKeys(YsonAsVariant)} needs Spark ${SchemaConverterConfig.VARIANT_SPARK_VERSION} or later. Set " +
      "it to false to read yson columns with their usual type; SQL yt.`path` reads it only from the settings the " +
      "Spark application starts with.")
  }
}

object SchemaConverterConfig {

  /** The first Spark version with the Variant type. */
  val VARIANT_SPARK_VERSION = "4.0.0"

  def apply(spark: SparkSession): SchemaConverterConfig = {
    SchemaConverterConfig(
      parsingTypeV3 = spark.ytConf(SparkYtConfiguration.Read.TypeV3),
      ysonAsVariant = spark.ytConf(YsonAsVariant))
  }

  def apply(conf: SparkConf): SchemaConverterConfig = {
    SchemaConverterConfig(
      parsingTypeV3 = conf.ytConf(SparkYtConfiguration.Read.TypeV3),
      ysonAsVariant = conf.ytConf(YsonAsVariant))
  }
}

