package tech.ytsaurus.spyt.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf.FILE_COMMIT_PROTOCOL_CLASS
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.yt.test.Utils
import org.apache.spark.yt.test.Utils.{SparkConfigEntry, defaultConfValue}
import org.scalatest.TestSuite
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter._
import tech.ytsaurus.spyt.wrapper.config.ConfigEntry
import tech.ytsaurus.spyt.test.LocalSpark.defaultSparkConf
import tech.ytsaurus.spyt.wrapper.client.{YtClientProvider, YtRpcClient}

import scala.annotation.tailrec

trait LocalSpark extends LocalYtClient {
  self: TestSuite =>
  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  def numExecutors: Int = 4

  def numFailures: Int = 1

  def sparkConf: SparkConf = defaultSparkConf

  def reinstantiateSparkSession: Boolean = false

  lazy val spark: SparkSession = {
    if (reinstantiateSparkSession) {
      LocalSpark.stop()
    }
    if (LocalSpark.spark != null) {
      LocalSpark.spark
    } else {
      LocalSpark.spark = sparkSessionBuilder.getOrCreate()
      LocalSpark.spark
    }
  }

  protected def sparkSessionBuilder: SparkSession.Builder = SparkSession.builder()
    .master(s"local[$numExecutors, $numFailures]")
    .config(sparkConf)

  override protected def ytRpcClient: YtRpcClient = {
    YtClientProvider.ytRpcClient(ytClientConfiguration(spark))
  }

  def physicalPlan(df: DataFrame): SparkPlan = {
    spark.sessionState.executePlan(df.queryExecution.logical)
      .executedPlan
  }

  def adaptivePlan(df: DataFrame): SparkPlan = {
    val plan = physicalPlan(df).asInstanceOf[AdaptiveSparkPlanExec]
    plan.execute()
    plan.executedPlan
  }

  def nodes(plan: SparkPlan): Seq[SparkPlan] = {
    @tailrec
    def inner(res: Seq[SparkPlan], queue: Seq[SparkPlan]): Seq[SparkPlan] = {
      queue match {
        case h :: t =>
          val children = h.children
          inner(res ++ children, t ++ children)
        case Nil => res
      }
    }

    inner(Seq(plan), Seq(plan))
  }

  def withConf[T, R](entry: ConfigEntry[T], value: T)(f: => R): R = {
    withConf(s"spark.yt.${entry.name}", value.toString, entry.default.map(_.toString))(f)
  }

  def withConf[T, R](entry: SparkConfigEntry[T], value: String)(f: => R): R = {
    withConf(entry.key, value, defaultConfValue(entry, LocalSpark.defaultSparkConf))(f)
  }

  def withConf[R](key: String, value: String, default: Option[String] = None)(f: => R): R = {
    val prev = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try {
      f
    } finally {
      default.orElse(prev) match {
        case Some(value) => spark.conf.set(key, value)
        case None => spark.conf.unset(key)
      }
    }
  }

  def withConfs[R](confs: Map[String, String])(f: => R): R = {
    def withConfRec(confs: Seq[(String, String)]): R = {
      confs match {
        case (headK, headV) +: tail =>
          withConf(headK, headV) {
            withConfRec(tail)
          }
        case _ =>
          f
      }
    }
    withConfRec(confs.toSeq)
  }

  def defaultParallelism: Int = Utils.defaultParallelism(spark)
}

object LocalSpark {
  private var spark: SparkSession = _

  def stop(): Unit = {
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
    spark = null
  }

  def maybeSetExtensions(sparkConf: SparkConf): SparkConf = {
    val extensionsClass = "tech.ytsaurus.spyt.format.YtSparkExtensions"
    val loader = getClass.getClassLoader
    try {
      loader.loadClass(extensionsClass)
      sparkConf.set("spark.sql.extensions", extensionsClass)
    } catch {
      case _: ClassNotFoundException => sparkConf
    }
  }

  val defaultSparkConf: SparkConf = maybeSetExtensions(new SparkConf())
    .set("fs.ytTable.impl", "tech.ytsaurus.spyt.fs.YtTableFileSystem")
    .set("fs.yt.impl", "tech.ytsaurus.spyt.fs.YtFileSystem")
    .set("spark.hadoop.fs.AbstractFileSystem.yt.impl", "tech.ytsaurus.spyt.fs.YtFs")
    .set("spark.hadoop.fs.yt.impl", "tech.ytsaurus.spyt.fs.YtFileSystem")
    .set("fs.defaultFS", "ytTable:///")
    .set("spark.hadoop.yt.proxy", LocalYt.proxy)
    .set("spark.hadoop.yt.user", "root")
    .set("spark.hadoop.yt.token", "")
    .set("spark.hadoop.yt.timeout", "300")
    .set("spark.yt.write.batchSize", "10")
    .set(FILE_COMMIT_PROTOCOL_CLASS.key, "tech.ytsaurus.spyt.format.DelegatingOutputCommitProtocol")
    .set("spark.ui.enabled", "false")
    .set("spark.hadoop.yt.read.arrow.enabled", "true")
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.yt.log.enabled", "false")
    .set("spark.datasource.yt.recursiveFileLookup", "true")
    .set("spark.hadoop.fs.null.impl", "tech.ytsaurus.spyt.fs.YtTableFileSystem")
    .set("spark.sql.caseSensitive", "true")
    .set("spark.sql.session.timeZone", "UTC")
}
