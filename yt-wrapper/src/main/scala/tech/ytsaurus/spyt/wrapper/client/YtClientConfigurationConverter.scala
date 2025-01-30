package tech.ytsaurus.spyt.wrapper.client

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import tech.ytsaurus.spyt.wrapper.config._

object YtClientConfigurationConverter {
  def ytClientConfiguration(spark: SparkSession): YtClientConfiguration = {
    ytClientConfiguration(spark.sparkContext.hadoopConfiguration)
  }

  def ytClientConfiguration(conf: Configuration): YtClientConfiguration = {
    YtClientConfiguration(conf.getYtConf(_))
  }

  def ytClientConfiguration(conf: SparkConf): YtClientConfiguration = {
    ytClientConfiguration(hadoopConf(conf.getAll))
  }

  def ytClientConfiguration(sqlConf: SQLConf): YtClientConfiguration = {
    ytClientConfiguration(hadoopConf(sqlConf.getAllConfs.toArray))
  }

  private def hadoopConf(conf: Array[(String, String)]): Configuration = {
    val hadoopConf = new Configuration()
    for ((key, value) <- conf if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }
    hadoopConf
  }
}
