package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.yt.YtSourceScanExec
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

class YtSourceStrategyTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with MockitoSugar
  with TableDrivenPropertyChecks with YtDistributedReadingTestUtils {

  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()


  def createTestTable(): Unit = {
    writeTableFromYson(Seq(
      """{a = 1; b = "x"; c = 0.1}""",
      """{a = 2; b = "y"; c = 0.2}""",
      """{a = 3; b = "z"; c = 0.3}"""
    ), tmpPath, atomicSchema)
  }

  private def collectScanNodes(plan: SparkPlan): Seq[SparkPlan] = {
    val unwrapped = plan match {
      case adaptive: AdaptiveSparkPlanExec =>
        adaptive.executedPlan
      case other => other
    }
    unwrapped.collect {
      case scan: YtSourceScanExec => scan
      case scan: FileSourceScanExec => scan
    }
  }

  behavior of "YtSourceStrategy plan structure"

  it should "produce YtSourceScanExec for SQL queries" in {
    createTestTable()
    val df = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`")
    df.collect()

    val scanNodes = collectScanNodes(df.queryExecution.executedPlan)
    scanNodes should not be empty
    all(scanNodes) shouldBe a[YtSourceScanExec]
  }

  it should "produce YtSourceScanExec for queries with filters" in {
    createTestTable()
    val df = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath` WHERE a > 1")
    df.collect()

    val scanNodes = collectScanNodes(df.queryExecution.executedPlan)
    scanNodes should not be empty
    all(scanNodes) shouldBe a[YtSourceScanExec]
  }

  it should "produce YtSourceScanExec for queries with projections" in {
    createTestTable()
    val df = spark.sql(s"SELECT a, b FROM yt.`ytTable:/$tmpPath`")
    df.collect()

    val scanNodes = collectScanNodes(df.queryExecution.executedPlan)
    scanNodes should not be empty
    all(scanNodes) shouldBe a[YtSourceScanExec]
  }

  it should "produce YtSourceScanExec for join queries" in {
    YtWrapper.createDir(tmpPath)
    val path1 = s"$tmpPath/t1"
    val path2 = s"$tmpPath/t2"

    writeTableFromYson(Seq("""{a = 1; b = "x"; c = 0.1}"""), path1, atomicSchema)
    writeTableFromYson(Seq("""{a = 1; b = "y"; c = 0.2}"""), path2, atomicSchema)

    val df = spark.sql(
      s"""SELECT t1.a, t2.b
         |FROM yt.`ytTable:/$path1` t1
         |JOIN yt.`ytTable:/$path2` t2 ON t1.a = t2.a""".stripMargin)
    df.collect()

    val scanNodes = collectScanNodes(df.queryExecution.executedPlan)
    scanNodes should not be empty
    all(scanNodes) shouldBe a[YtSourceScanExec]
  }

  behavior of "YtSourceStrategy case sensitivity handling"

  it should "handle case-insensitive column references in filters" in {
    createTestTable()
    withConf("spark.sql.caseSensitive", "false") {
      val res = spark.sql(f"SELECT * FROM yt.`ytTable:/$tmpPath` WHERE A > 1")
      res.collect() should contain theSameElementsAs Seq(
        Row(2, "y", 0.2),
        Row(3, "z", 0.3)
      )
    }
  }

  it should "handle case-insensitive column references in projections" in {
    createTestTable()
    withConf("spark.sql.caseSensitive", "false") {
      val res = spark.sql(f"SELECT A, B FROM yt.`ytTable:/$tmpPath`")
      res.collect() should contain theSameElementsAs Seq(
        Row(1, "x"),
        Row(2, "y"),
        Row(3, "z")
      )
    }
  }

  it should "handle mixed-case column references in complex expressions" in {
    createTestTable()
    withConf("spark.sql.caseSensitive", "false") {
      val res = spark.sql(f"SELECT A, C FROM yt.`ytTable:/$tmpPath` WHERE a >= 2 AND c < 0.25")
      res.collect() should contain theSameElementsAs Seq(Row(2, 0.2))
    }
  }
}
