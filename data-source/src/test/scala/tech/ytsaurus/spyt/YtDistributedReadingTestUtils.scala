package tech.ytsaurus.spyt

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.v2.Utils.extractYtScan
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test.{LocalSpark, LocalYtClient, TmpDir}

trait YtDistributedReadingTestUtils extends AnyFlatSpec with Matchers with LocalSpark with TmpDir {
  self: LocalYtClient =>

  def distributedReadingEnabledConf(distributedReadingEnabled: Boolean): Map[String, String] = Map(
    s"spark.yt.${SparkYtConfiguration.Read.YtDistributedReadingEnabled.name}" -> distributedReadingEnabled.toString,
    s"spark.yt.${SparkYtConfiguration.Read.PlanOptimizationEnabled.name}" -> {!distributedReadingEnabled}.toString,
  )

  def testWithDistributedReading(testName: String, tag: Option[Tag] = None)(testBody: Boolean => Unit): Unit = {
    val test1Base = it should s"$testName: distributedReadingEnabled = false"
    val test2Base = it should s"$testName: distributedReadingEnabled = true"
    val List(test1, test2) =
      List(test1Base, test2Base).map(t => if (tag.isDefined) t.taggedAs(tag.get).in(_) else t.in(_))

    test1 {
      testBody(false)
    }

    test2 {
      withConfs(distributedReadingEnabledConf(true)) {
        testBody(true)
      }
    }
  }

  def getDelegatesForTable(spark: SparkSession, tmpPath: String): Seq[YtPartitionedFileDelegate] = {
    getDelegates(spark.read.yt(tmpPath))
  }

  def getDelegates(df: DataFrame): Seq[YtPartitionedFileDelegate] = {
    df.collect()
    extractYtScan(df.queryExecution.executedPlan).getPartitions
      .flatMap(_.files.map(_.asInstanceOf[YtPartitionedFileBase[YtPartitionedFileDelegate]].delegate))
  }
}
