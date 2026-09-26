package tech.ytsaurus.spyt.format.columnar

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.ColumnarUdfEnabled
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.types.UInt64Long
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import scala.jdk.CollectionConverters._

/**
 * Integration coverage for Arrow plugin planning, results, and task resource ownership.
 *
 * Run through the data-source-extended Gradle test task with Spark 4.2.0 and Scala 2.13, using the
 * local YT test fixture for scan-optimized tables. The pure Java test provider exercises normal
 * evaluation, invalid outputs, retained buffers, and early termination without a native library.
 */
class YtColumnarFunctionExecTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils {

  /** Isolates the startup parser configuration from other tests and suites. */
  override def reinstantiateSparkSession: Boolean = true

  /** Enables parser registration before the session is built; each test separately enables the execution rule. */
  override protected def sparkSessionBuilder(extraConf: Map[String, String]): SparkSession.Builder = {
    super.sparkSessionBuilder(extraConf + (ColumnarUdfEnabled.name -> "true"))
  }

  behavior of "Columnar function plugins"

  /** Registers the shared test provider as `test_vector_sum`, using the supplied JSON options. */
  private def register(options: String = "{}"): Unit = {
    spark.sql(s"REGISTER COLUMNAR FUNCTION test_vector_sum AS '${classOf[TestColumnarFunctionProvider].getName}' " +
      s"OPTIONS '$options'").collect()
  }

  /** Finds plugin operators in an executed plan, descending through adaptive plans and query stages. */
  private def operators(plan: SparkPlan): Seq[YtColumnarUdfExec] = plan match {
    case adaptive: AdaptiveSparkPlanExec => operators(adaptive.executedPlan)
    case stage: QueryStageExec => operators(stage.plan)
    case op: YtColumnarUdfExec => Seq(op) ++ op.children.flatMap(operators)
    case other => other.children.flatMap(operators)
  }

  /** Checks after a completed query that every created evaluator and tracked output buffer was released. */
  private def assertClosed(): Unit = {
    TestColumnarFunctionProvider.CREATED.get() should be > 0
    TestColumnarFunctionProvider.CLOSED.get() shouldBe TestColumnarFunctionProvider.CREATED.get()
    TestColumnarFunctionProvider.OUTPUTS.asScala.foreach { root =>
      root.getFieldVectors.asScala.foreach(_.getDataBuffer.capacity() shouldBe 0L)
    }
  }

  for (adaptive <- Seq(false, true); codegen <- Seq(false, true)) {
    it should s"evaluate multiple nullable inputs with row transitions, AQE=$adaptive and codegen=$codegen" in {
      whenSparkVersionAtLeast("4.2.0") {
        withConfs(Map(ColumnarUdfEnabled.name -> "true",
          "spark.sql.adaptive.enabled" -> adaptive.toString,
          "spark.sql.codegen.wholeStage" -> codegen.toString)) {
          register()
          TestColumnarFunctionProvider.reset()
          val rows = Seq(Row(1L, 2L), Row(null, 3L), Row(4L, null), Row(-8L, 10L))
          val input = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2),
            StructType(Seq(StructField("a", LongType), StructField("b", LongType))))
          val result = input.repartition(2).selectExpr("a", "b", "test_vector_sum(a, b) as total")
          result.collect().toSeq should contain theSameElementsAs
            Seq(Row(1L, 2L, 3L), Row(null, 3L, null), Row(4L, null, null), Row(-8L, 10L, 2L))
          val columnar = operators(result.queryExecution.executedPlan)
          columnar should have size 1
          columnar.head.child.isInstanceOf[RowToColumnarExec] shouldBe true
          input.repartition(2).selectExpr("test_vector_sum(a, b)", "b", "b as repeated", "a").collect().toSeq should
            contain theSameElementsAs Seq(
              Row(3L, 2L, 2L, 1L), Row(null, 3L, 3L, null), Row(null, null, null, 4L), Row(2L, 10L, 10L, -8L))
          assertClosed()
        }
      }
    }
  }

  it should "consume scan-optimized YT Arrow batches and preserve pass-through columns" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register()
        TestColumnarFunctionProvider.reset()
        val schema = TableSchema.builder().addValue("a", ColumnValueType.INT64)
          .addValue("b", ColumnValueType.INT64).addValue("label", ColumnValueType.STRING).build()
        writeTableFromYson(Seq("{a=1;b=2;label=first}", "{a=#;b=3;label=second}",
          "{a=-4;b=9;label=third}"), tmpPath, schema, OptimizeMode.Scan)
        val result = spark.read.yt(tmpPath).selectExpr("a", "b", "label", "test_vector_sum(a, b)")
        result.collect().toSeq should contain theSameElementsAs
          Seq(Row(1L, 2L, "first", 3L), Row(null, 3L, "second", null), Row(-4L, 9L, "third", 5L))
        val columnar = operators(result.queryExecution.executedPlan)
        columnar should have size 1
        columnar.head.child.supportsColumnar shouldBe true
        columnar.head.child.isInstanceOf[RowToColumnarExec] shouldBe false
        assertClosed()
      }
    }
  }

  it should "evaluate uint64 inputs from scan-optimized YT Arrow batches and preserve pass-through columns" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register("""{"mode":"uint64"}""")
        TestColumnarFunctionProvider.reset()
        val schema = TableSchema.builder().addValue("a", ColumnValueType.UINT64)
          .addValue("b", ColumnValueType.UINT64).addValue("label", ColumnValueType.STRING).build()
        writeTableFromYson(Seq("{a=1u;b=2u;label=small}", "{a=#;b=3u;label=null_left}",
          "{a=4u;b=#;label=null_right}", "{a=9223372036854775808u;b=1u;label=large}",
          "{a=18446744073709551615u;b=0u;label=max}"), tmpPath, schema, OptimizeMode.Scan)
        val result = spark.read.yt(tmpPath).selectExpr("a", "b", "label", "test_vector_sum(a, b) as total")
        Seq("a", "b", "total").foreach(name => result.schema(name).dataType shouldBe UInt64Type)
        result.collect().toSeq should contain theSameElementsAs Seq(
          Row(UInt64Long(1L), UInt64Long(2L), "small", UInt64Long(3L)),
          Row(null, UInt64Long(3L), "null_left", null),
          Row(UInt64Long(4L), null, "null_right", null),
          Row(UInt64Long("9223372036854775808"), UInt64Long(1L), "large", UInt64Long("9223372036854775809")),
          Row(UInt64Long("18446744073709551615"), UInt64Long(0L), "max", UInt64Long("18446744073709551615")))
        val columnar = operators(result.queryExecution.executedPlan)
        columnar should have size 1
        columnar.head.child.supportsColumnar shouldBe true
        columnar.head.child.isInstanceOf[RowToColumnarExec] shouldBe false
        assertClosed()
      }
    }
  }

  it should "release evaluator and output resources when LIMIT stops consuming batches" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register()
        TestColumnarFunctionProvider.reset()
        spark.range(100000).selectExpr("test_vector_sum(id, id)").limit(1).collect().toSeq shouldBe Seq(Row(0L))
        assertClosed()
      }
    }
  }

  it should "retain Arrow input buffers in plugin outputs across scan batches and release them on LIMIT" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register("""{"mode":"identity"}""")
        TestColumnarFunctionProvider.reset()
        val schema = TableSchema.builder().addValue("a", ColumnValueType.INT64)
          .addValue("b", ColumnValueType.INT64).build()
        (0 until 4).foreach { chunk =>
          val rows = (0 until 20).map(row => s"{a=${chunk * 20 + row};b=${100 + row}}") :+ "{a=#;b=1}"
          if (chunk == 0) writeTableFromYson(rows, tmpPath, schema, OptimizeMode.Scan)
          else overwriteTableFromYson(rows, tmpPath, schema, append = true)
        }
        val result = spark.read.yt(tmpPath).selectExpr("a", "test_vector_sum(a, b)")
        result.collect().toSeq should contain theSameElementsAs
          ((0L until 80).map(value => Row(value, value)) ++ Seq.fill(4)(Row(null, null)))
        TestColumnarFunctionProvider.SHARED_OUTPUTS.get() should be > 1
        operators(result.queryExecution.executedPlan).head.child.isInstanceOf[RowToColumnarExec] shouldBe false
        assertClosed()
        TestColumnarFunctionProvider.reset()
        result.limit(1).collect() should have length 1
        TestColumnarFunctionProvider.SHARED_OUTPUTS.get() should be > 0
        assertClosed()
      }
    }
  }

  it should "avoid creating plugin evaluators for empty input" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register()
        TestColumnarFunctionProvider.reset()
        spark.range(0).selectExpr("test_vector_sum(id, id)").collect() shouldBe empty
        TestColumnarFunctionProvider.CREATED.get() shouldBe 0
        TestColumnarFunctionProvider.CLOSED.get() shouldBe 0
        TestColumnarFunctionProvider.OUTPUTS.isEmpty shouldBe true
      }
    }
  }

  it should "expose a nullable struct result as a Spark column" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register("""{"mode":"struct"}""")
        val input = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(2L, 3L), Row(null, 1L))),
          StructType(Seq(StructField("a", LongType), StructField("b", LongType))))
        input.selectExpr("test_vector_sum(a, b)").collect().toSeq should
          contain theSameElementsAs Seq(Row(Row(5L, 6L)), Row(null))
      }
    }
  }

  for (mode <- Seq("wrong-count", "wrong-schema", "fail")) {
    it should s"reject $mode and close the evaluator and allocated output" in {
      whenSparkVersionAtLeast("4.2.0") {
        withConf(ColumnarUdfEnabled.name, "true") {
          register(s"""{"mode":"$mode"}""")
          TestColumnarFunctionProvider.reset()
          intercept[Exception] {
            spark.range(3).coalesce(1).selectExpr("test_vector_sum(id, id)").collect()
          }
          assertClosed()
        }
      }
    }
  }

  it should "reject incompatible inputs and compose results with arithmetic" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register()
        intercept[Exception] {
          spark.range(3).selectExpr("test_vector_sum(id)").collect()
        }
        intercept[Exception] {
          spark.range(3).selectExpr("test_vector_sum(cast(id as string), id)").collect()
        }
        spark.range(3).selectExpr("test_vector_sum(id, id) + 1L").collect().toSeq should
          contain theSameElementsAs Seq(Row(1L), Row(3L), Row(5L))
      }
    }
  }
}
