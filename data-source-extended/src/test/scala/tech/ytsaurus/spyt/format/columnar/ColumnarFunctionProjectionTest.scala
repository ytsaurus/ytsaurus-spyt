package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.vector.FieldVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, GreaterThan, Literal}
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.ColumnarUdfEnabled
import tech.ytsaurus.spyt.test.LocalSpark

import scala.jdk.CollectionConverters._

/**
 * Exercises plugin projection composition and resource failures using local Spark range inputs.
 * Run with Spark 4.2.0 or later; this suite needs neither YT tables nor a native library.
 */
class ColumnarFunctionProjectionTest extends AnyFlatSpec with LocalSpark with Matchers {

  /** Isolates the startup parser configuration from other tests and suites. */
  override def reinstantiateSparkSession: Boolean = true

  /** Enables parser registration before the session is built; each test separately enables the execution rule. */
  override protected def sparkSessionBuilder(extraConf: Map[String, String]): SparkSession.Builder = {
    super.sparkSessionBuilder(extraConf + (ColumnarUdfEnabled.name -> "true"))
  }

  behavior of "Columnar function projections"

  /** Registers an isolated function name with options selecting the Java fixture's behavior. */
  private def register(name: String, options: String = "{}"): Unit = {
    spark.sql(s"REGISTER COLUMNAR FUNCTION $name AS '${classOf[TestColumnarFunctionProvider].getName}' " +
      s"OPTIONS '$options'").collect()
  }

  /** Checks that all successfully created evaluators, output buffers, and partition allocators were closed. */
  private def assertClosed(): Unit = {
    TestColumnarFunctionProvider.CREATED.get() should be > 0
    TestColumnarFunctionProvider.CLOSED.get() shouldBe TestColumnarFunctionProvider.CREATED.get()
    TestColumnarFunctionProvider.OUTPUTS.asScala.foreach { root =>
      root.getFieldVectors.asScala.foreach(assertBuffersReleased)
    }
    TestColumnarFunctionProvider.ALLOCATORS.asScala.foreach { allocator =>
      allocator.getAllocatedMemory shouldBe 0L
      // Arrow's assertOpen checks closed state when assertions are enabled by the test runner.
      org.apache.arrow.memory.util.AssertionUtil.ASSERT_ENABLED shouldBe true
      intercept[IllegalStateException](allocator.assertOpen())
    }
  }

  /** Checks each vector's owned buffers, including child buffers of structs, lists, and maps. */
  private def assertBuffersReleased(vector: FieldVector): Unit = {
    vector.getFieldBuffers.asScala.foreach(_.capacity() shouldBe 0L)
    vector.getChildrenFromFields.asScala.foreach(assertBuffersReleased)
  }

  it should "identify the unsupported operator and its columnar expression" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        val child = spark.range(0, 5).queryExecution.sparkPlan
        val descriptor = new TestColumnarFunctionProvider().describe(java.util.Collections.emptyMap[String, String]())
        val function = YtColumnarFunction(Seq(child.output.head, child.output.head),
          classOf[TestColumnarFunctionProvider].getName, Map.empty, descriptor.inputSchema().toJson,
          descriptor.outputSchema().toJson, LongType, "unsupported_sum", true)
        val filter = FilterExec(GreaterThan(function, Literal(0L)), child)
        val error = intercept[UnsupportedColumnarFunctionException] {
          new YtColumnarUdfRule(spark).preColumnarTransitions.apply(filter)
        }
        error.getMessage should include(filter.nodeName)
        error.getMessage should include(filter.condition.sql)
        error.getMessage should include("Move these calls into a projection")
      }
    }
  }

  for (codegen <- Seq(false, true)) {
    it should
      s"compose sibling arithmetic, outer aliases, computed arguments and nested calls with codegen=$codegen" in {
      whenSparkVersionAtLeast("4.2.0") {
        withConfs(Map(ColumnarUdfEnabled.name -> "true",
          "spark.sql.codegen.wholeStage" -> codegen.toString)) {
          register("compose_sum")
          TestColumnarFunctionProvider.reset()
          val input = spark.range(0, 5, 1, 1)
          input.selectExpr("compose_sum(id, id)", "id + 1L").collect().toSeq shouldBe
            (0L until 5).map(i => Row(i * 2, i + 1))
          input.selectExpr("compose_sum(id, id) as x").selectExpr("x + 1L").collect().toSeq shouldBe
            (0L until 5).map(i => Row(i * 2 + 1))
          input.selectExpr("compose_sum(id + 1L, id * 3L) + 2L").collect().toSeq shouldBe
            (0L until 5).map(i => Row(i * 4 + 3))
          input.selectExpr("compose_sum(compose_sum(id, id), id) as nested").collect().toSeq shouldBe
            (0L until 5).map(i => Row(i * 3))
          assertClosed()
        }
      }
    }
  }

  for (batchCount <- Seq(1, 2)) {
    it should s"preserve returned batches across repeated hasNext calls with $batchCount batches" in {
      whenSparkVersionAtLeast("4.2.0") {
        withConf(ColumnarUdfEnabled.name, "true") {
          TestColumnarFunctionProvider.reset()
          val attribute = AttributeReference("value", LongType, nullable = false)()
          val descriptor = new TestColumnarFunctionProvider().describe(java.util.Collections.emptyMap[String, String]())
          val function = YtColumnarFunction(Seq(attribute, attribute), classOf[TestColumnarFunctionProvider].getName,
            Map.empty, descriptor.inputSchema().toJson, descriptor.outputSchema().toJson, LongType, "sum", true)
          val input = spark.sparkContext.parallelize(Seq(batchCount), 1).mapPartitions { counts =>
            new Iterator[ColumnarBatch] {

              private val count = counts.next()
              private var index = 0
              private var previous: Option[ColumnarBatch] = None

              /** Models a scan reader that releases its previous vectors while probing the next batch. */
              override def hasNext: Boolean = {
                previous.foreach { batch =>
                  batch.column(0).asInstanceOf[OnHeapColumnVector].putLong(0, -1L)
                  batch.close()
                }
                previous = None
                index < count
              }

              /** Creates a source vector whose value must survive the next availability probe. */
              override def next(): ColumnarBatch = {
                if (!hasNext) throw new NoSuchElementException
                index += 1
                val vector = new OnHeapColumnVector(1, LongType)
                vector.putLong(0, index.toLong)
                val batch = new ColumnarBatch(Array(vector), 1)
                previous = Some(batch)
                batch
              }
            }
          }
          val plan = YtColumnarUdfExec(Seq(attribute, Alias(function, "sum")()),
            ColumnarIteratorInput(Seq(attribute), input))
          val checked = plan.executeColumnar().mapPartitions { batches =>
            var index = 0
            while (index < batchCount) {
              val batch = batches.next()
              index += 1
              assert(batch.column(0).getLong(0) == index.toLong)
              assert(batch.column(1).getLong(0) == index * 2L)
              (0 until 3).foreach { _ =>
                assert(batches.hasNext == (index < batchCount))
                assert(batch.column(0).getLong(0) == index.toLong)
                assert(batch.column(1).getLong(0) == index * 2L)
              }
            }
            Iterator.single(index)
          }.collect()
          checked.toSeq shouldBe Seq(batchCount)
          assertClosed()
        }
      }
    }
  }

  it should "preserve distinct configured outputs across multiple batches" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConfs(Map(ColumnarUdfEnabled.name -> "true",
        "spark.sql.inMemoryColumnarStorage.batchSize" -> "1024")) {
        register("first_sum")
        register("offset_sum", """{"offset":"100"}""")
        TestColumnarFunctionProvider.reset()
        val rows = spark.range(0, 10000, 1, 1)
          .selectExpr("first_sum(id, id) as first", "offset_sum(id, id) as second", "id + 1L").collect().toSeq
        rows shouldBe (0L until 10000).map(i => Row(i * 2, i * 2 + 100, i + 1))
        TestColumnarFunctionProvider.CREATED.get() shouldBe 2
        TestColumnarFunctionProvider.OUTPUTS.size() should be > 2
        assertClosed()
      }
    }
  }

  it should "create independent evaluators for repeated nondeterministic calls" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register("independent_sum", """{"mode":"nondeterministic"}""")
        TestColumnarFunctionProvider.reset()
        val rows = spark.range(0, 3, 1, 1)
          .selectExpr("independent_sum(id, id) as first", "independent_sum(id, id) as second").collect().toSeq
        rows.zipWithIndex.foreach { case (row, index) =>
          Set(row.getLong(0), row.getLong(1)) shouldBe Set(index * 2L + 1, index * 2L + 2)
        }
        TestColumnarFunctionProvider.CREATED.get() shouldBe 2
        assertClosed()
      }
    }
  }

  it should "accept a null struct parent with unset required child slots" in {
    whenSparkVersionAtLeast("4.2.0") {
      withConf(ColumnarUdfEnabled.name, "true") {
        register("null_parent_sum", """{"mode":"null-parent"}""")
        TestColumnarFunctionProvider.reset()
        spark.range(0, 3, 1, 1).selectExpr("null_parent_sum(id, id)").collect().toSeq shouldBe
          Seq(Row(null), Row(Row(2L, 1L)), Row(Row(4L, 4L)))
        assertClosed()
      }
    }
  }

  for (mode <- Seq("create-fail", "close-fail")) {
    it should s"close other evaluators and owned buffers when $mode throws" in {
      whenSparkVersionAtLeast("4.2.0") {
        withConf(ColumnarUdfEnabled.name, "true") {
          register("healthy_sum")
          register("failing_sum", s"""{"mode":"$mode"}""")
          TestColumnarFunctionProvider.reset()
          intercept[Exception] {
            spark.range(0, 3, 1, 1).selectExpr("healthy_sum(id, id)", "failing_sum(id, id)").collect()
          }
          TestColumnarFunctionProvider.ALLOCATORS.size() shouldBe 2
          assertClosed()
        }
      }
    }
  }
}

/** Supplies a controlled batch iterator to test columnar execution without row transitions. */
private case class ColumnarIteratorInput(override val output: Seq[Attribute], batches: RDD[ColumnarBatch])
  extends LeafExecNode {

  /** Keeps Spark on the columnar path for this test source. */
  override def supportsColumnar: Boolean = true

  /** Returns the deliberately stateful source iterator. */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = batches

  /** Rejects accidental row execution of this test fixture. */
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
}
