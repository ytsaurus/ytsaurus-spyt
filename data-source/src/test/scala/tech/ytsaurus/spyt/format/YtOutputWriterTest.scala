package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.unsafe.types.UTF8String
import org.mockito.scalatest.MockitoSugar
import org.mockito.Mockito.{when => mWhen}
import org.mockito.ArgumentMatchers.{anyInt => mAnyInt}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.ApiServiceTransaction
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{CustomAttribute, SortColumns, SortOrders, UniqueKeys}
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Sorted, Unordered}
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.{YtReadContext, YtReadSettings}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDate}

class YtOutputWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with MockitoSugar {
  import YtOutputWriterTest._
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  implicit val ytReadContext: YtReadContext = YtReadContext(yt, YtReadSettings.default)

  private val sqlImplicits = SparkAdapter.instance.sparkImplicits(spark)
  import sqlImplicits._

  it should "exception while writing several batches with relative in path" in {
    an[IllegalArgumentException] shouldBe thrownBy {
      prepareWrite("subfolder", Unordered) { transaction => }
    }
  }

  it should "not write several batches if table is sorted" in {
    prepareWrite(tmpPath, Sorted(Seq("a"), uniqueKeys = false)) { transaction =>
      val p = YPathEnriched.fromString(tmpPath).withTransaction(transaction.getId.toString)
      val writer = new MockYtOutputWriter(p, 2, Sorted(Seq("a"), uniqueKeys = false))
      val rows = Seq(Row(1), Row(2), Row(3), Row(4))

      writeRows(rows, writer, transaction)

      spark.read.yt(tmpPath).collect() should contain theSameElementsAs rows
      YtWrapper.chunkCount(tmpPath) shouldEqual 1
    }
  }

  it should "use YtOutputWriter if used via spark.write.format(yt) for static tables" in {
    val sampleData = (1 to 1000).map(n => SampleRow(n.longValue() * n, 1.0 + 1.7*n, s"$n-th row"))

    val df = spark.createDataset(sampleData)
    df.write.format("yt").save("ytTable:/" + tmpPath)

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))
    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])

    outputPathAttributes("dynamic").boolValue() shouldBe false

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData)(_.getValues.get(0).longValue())
  }

  it should "be able to configure table writer" in {
    val sampleData = Seq(SampleRow(1, 1.0, "F" * (16 << 20)))

    val df = spark.createDataset(sampleData)
    df.write.format("yt").option("table_writer", "{max_row_weight=20000000}").save("ytTable:/" + tmpPath)

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData)(_.getValues.get(0).longValue())
  }

  it should "correctly serialize time to YSON" in {
    val sampleData = (1 to 1000).map(n => SampleRow2(Nested(
      java.sql.Timestamp.from(Instant.now().minusSeconds(n).truncatedTo(ChronoUnit.MICROS)),
      java.sql.Date.valueOf(LocalDate.now().minusDays(n)),
      org.apache.spark.sql.spyt.types.Date32.apply(LocalDate.now().minusDays(n)),
      org.apache.spark.sql.spyt.types.Datetime.apply(java.time.LocalDateTime.now().minusDays(n).withNano(0)),
    )))

    spark.createDataset(sampleData).write.option("write_type_v3", "true").yt(tmpPath)

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))
    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])

    outputPathAttributes("dynamic").boolValue() shouldBe false

    val result = spark.read
      .option(YtUtils.Options.PARSING_TYPE_V3, value = true)
      .yt(tmpPath)
      .select($"nested.*")
      .as[Nested]
      .collect()

    result should contain theSameElementsAs sampleData.map(_.nested)
  }

  it should "write custom options to attributes" in {
    val sample = spark.range(100).select(col("id"), concat(lit("id = "), col("id")).as("a"))

    sample.write.option("attr_custom_int", 10)
      .option("attr_custom_boolean", "%false")
      .option("attr_custom_yson", "{key1=1; key2=2}")
      .option("attr_attr_string", "101-102")
      .option("attr_", "value")
      .option("attr_empty", "")
      .option("attr_write_type_v3", value = true)
      .yt(tmpPath)

    val outputPathAttributes = YtWrapper.attributes(tmpPath, None, Set.empty[String])

    outputPathAttributes("custom_int").intValue() shouldBe 10
    outputPathAttributes("custom_boolean").boolValue() shouldBe false
    outputPathAttributes("custom_yson").asMap()
      .equals(java.util.Map.of("key1", CustomAttribute.get("1"), "key2", CustomAttribute.get("2"))) shouldBe true
    outputPathAttributes("attr_string").stringValue() shouldBe "101-102"
    outputPathAttributes.keySet.contains("empty") shouldBe false
    outputPathAttributes.values.count(_.equals(CustomAttribute.get("value"))) shouldBe 0
    outputPathAttributes.keySet.contains("write_type_v3") shouldBe false
  }

  it should "write custom options to attributes with append" in {
    val sample = spark.range(50).select(col("id"), concat(lit("id = "), col("id")).as("a"))

    sample.write.option("attr_custom_string", "before_append").yt(tmpPath)

    val toAppend = spark.range(51, 100).select(col("id"), concat(lit("id = "), col("id")).as("a"))
    toAppend.write
      .option("attr_custom_string", "after_append")
      .option("attr_new_attribute", 10)
      .mode(SaveMode.Append)
      .yt(tmpPath)

    val outputPathAttributes = YtWrapper.attributes(tmpPath, None, Set.empty[String])

    outputPathAttributes("new_attribute").intValue() shouldBe 10
    outputPathAttributes("custom_string").stringValue() shouldBe "after_append"
  }

  def prepareWrite(path: String, sortOption: SortOption)
                  (f: ApiServiceTransaction => Unit): Unit = {
    val transaction = YtWrapper.createTransaction(parent = None, timeout = Duration.ofMinutes(1))
    val transactionId = transaction.getId.toString

    YtWrapper.createTable(path, TestTableSettings(schema, sortOption = sortOption),
      transaction = Some(transactionId))

    try {
      f(transaction)
    } catch {
      case e: Throwable =>
        try {
          transaction.abort().join()
        } catch {
          case ae: Throwable =>
            e.addSuppressed(ae)
        }
        throw e
    }
  }

  def writeRows(rows: Seq[Row], writer: YtOutputWriter, transaction: ApiServiceTransaction): Unit = {
    try {
      rows.foreach(r => writer.write(mockInternalRow(r)))
    } finally {
      try {
        writer.close()
      } finally {
        transaction.commit().join()
      }
    }
  }

  def mockInternalRow(row: Row): InternalRow = {
    val mockedRow = mock[InternalRow]
    mWhen(mockedRow.numFields).thenReturn(row.length)
    mWhen(mockedRow.copy()).thenReturn(mockedRow)
    mWhen(mockedRow.isNullAt(mAnyInt())).thenAnswer(inv => row.isNullAt(inv.getArgument[Int](0)))
    mWhen(mockedRow.getInt(mAnyInt())).thenAnswer(inv => row.getInt(inv.getArgument[Int](0)))
    mockedRow
  }

  class MockYtOutputWriter(path: YPathEnriched, batchSize: Int, sortOption: SortOption)
    extends YtOutputWriter(
      path,
      schema,
      SparkYtWriteConfiguration(1, batchSize, Duration.ofMinutes(5), typeV3Format = false, distributedWrite = false),
      Map("sort_columns" -> SortColumns.set(sortOption.keys),
        "sort_orders" -> SortOrders.set(sortOption.orders.map(_.toString)),
        "unique_keys" -> UniqueKeys.set(sortOption.uniqueKeys))
    ) {
    override protected def initialize(): Unit = {}
  }
}

object YtOutputWriterTest {
  case class SampleRow(id: Long, ratio: Double, value: String)
  case class Nested(
    timestamp: java.sql.Timestamp,
    date: java.sql.Date,
    date32: org.apache.spark.sql.spyt.types.Date32,
    dateTime: org.apache.spark.sql.spyt.types.Datetime,
  )
  case class SampleRow2(nested: Nested)

  implicit val sampleRowOrdering: Ordering[SampleRow] = Ordering.by(_.id)
}
