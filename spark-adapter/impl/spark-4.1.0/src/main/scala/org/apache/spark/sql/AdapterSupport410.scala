package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.Utils

import java.io.File
import scala.reflect.ClassTag

object AdapterSupport410 {

  def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    new ShuffleDependency[K, V, C](rdd, partitioner, serializer,
      shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))
  }

  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String] = {
    Utils.sparkJavaOpts(conf, filterKey)
  }

  def getHadoopConf(sparkConf: SparkConf): Configuration = {
    SparkHadoopUtil.newConfiguration(sparkConf)
  }

  def fetchFile(url: String, targetDir: File, conf: SparkConf): File = Utils.fetchFile(
    url,
    targetDir,
    conf,
    SparkHadoopUtil.get.newConfiguration(conf),
    System.currentTimeMillis(),
    useCache = false
  )
}
