package tech.ytsaurus.spyt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.v2.Utils.extractYtScan
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

  def testWithDistributedReading(testName: String)(testBody: Boolean => Unit): Unit = {
    it should s"$testName: distributedReadingEnabled = false" in {
      testBody(false)
    }

    it should s"$testName: distributedReadingEnabled = true" in {
      withConfs(distributedReadingEnabledConf(true)) {
        testBody(true)
      }
    }
  }

  def getPartitionsForTable(spark: SparkSession, tmpPath: String): Seq[FilePartition] = {
    val readTask = spark.read.yt(tmpPath)
    readTask.collect()
    val ytScan = extractYtScan(readTask.queryExecution.executedPlan)
    ytScan.getPartitions
  }

  def getDelegatesForTable(spark: SparkSession, tmpPath: String): Seq[YtPartitionedFileDelegate] = {
    getPartitionsForTable(spark, tmpPath)
      .flatMap(_.files.map(_.asInstanceOf[YtPartitionedFileBase[YtPartitionedFileDelegate]].delegate))
  }
}
