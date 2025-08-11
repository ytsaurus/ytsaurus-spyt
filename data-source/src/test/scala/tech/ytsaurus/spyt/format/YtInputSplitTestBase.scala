package tech.ytsaurus.spyt.format

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TestRow, TestUtils, TmpDir}
import tech.ytsaurus.spyt._

abstract class YtInputSplitTestBase extends AnyFlatSpec with Matchers with LocalSpark with DynTableTestUtils
  with TmpDir with TestUtils {
  behavior of "YtInputSplit"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyColumnsFilterPushdown.Enabled.name}", value = true)
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyColumnsFilterPushdown.YtPathCountLimit.name}", value = 10)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyColumnsFilterPushdown.Enabled.name}", value = false)
  }

  List("signed" -> testSchema, "unsigned" -> testSchemaUnsigned).foreach { case (name, schema) =>
    it should s"reduce number of read rows in dynamic tables for $name schema" in {
      prepareTestTable(
        tmpPath,
        (1L to 1000L).map(x => (x / 10, x % 10, 0.toString)).map { case (a, b, c) => TestRow(a, b, c) },
        Seq(Seq(), Seq(6, 0), Seq(7, 0), Seq(50), Seq(80, 0)),
        schema = schema,
      )

      val res = spark.read.option("enable_inconsistent_read", "true").yt(tmpPath)
      val test = Seq(
        (res("a") <= 50 && res("a") >= 50 - 1 && res("b") === 1L, 20L),
        (res("a") >= 77L && res("b").isin(0L) && res("c") === "0", 300L),
        (res("a") === 6, 20L),
        (res("a") < 10 || res("a") > 20, 950L),
        (res("a") < 10 && res("a") > 20, 0L),
        (res("a") < 50 && res("c") < "1", 550L),
        (res("a").isin(10, 20, 30, 49), 50L)
      )
      test.foreach { case (filter, rowLimit) => getNumOutputRows(res, filter) should be <= rowLimit }
    }
  }

  it should "not lose non-empty partitions when key columns are unsigned" in {
    prepareTestTable(
      tmpPath, (0L until 16L).map(x => (x << 60, x % 10, 0.toString)).map { case (a, b, c) => TestRow(a, b, c) },
      Seq(Seq(), Seq(Long.MaxValue / 2), Seq(Long.MinValue), Seq(Long.MinValue / 2)),
      schema = testSchemaUnsigned,
    )
    withConf(s"spark.yt.${SparkYtConfiguration.Read.YtPartitioningEnabled.name}", "true") {
      spark.read.yt(tmpPath).cache().count() shouldBe 16
    }
  }

  protected def getNumOutputRows(res: DataFrame, filter: Column): Long = {
    val query = res.filter(filter)
    query.collect()
    query.queryExecution.executedPlan.collectFirst {
      case b: BatchScanExec => b.metrics("numOutputRows").value
    }.get
  }
}
