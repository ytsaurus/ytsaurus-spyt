package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.client.ApiServiceTransaction
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{SortColumns, UniqueKeys}
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Sorted, Unordered}
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
import scala.concurrent.duration._
import scala.language.postfixOps

class YtOutputWriterTest extends FlatSpec with TmpDir with LocalSpark with Matchers {
  import YtOutputWriterTest._
  private val schema = StructType(Seq(StructField("a", IntegerType)))

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
    import spark.implicits._
    val sampleData = (1 to 1000).map(n => SampleRow(n.longValue() * n, 1.0 + 1.7*n, s"$n-th row"))

    val df = spark.createDataset(sampleData)
    df.write.format("yt").save("ytTable:/" + tmpPath)

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))
    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])

    outputPathAttributes("dynamic").boolValue() shouldBe false

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData)(_.getValues.get(0).longValue())
  }

  it should "be able to configure table writer" in {
    import spark.implicits._
    val sampleData = Seq(SampleRow(1, 1.0, "F" * (16 << 20)))

    val df = spark.createDataset(sampleData)
    df.write.format("yt").option("table_writer", "{max_row_weight=20000000}").save("ytTable:/" + tmpPath)

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData)(_.getValues.get(0).longValue())
  }

  it should "correctly serialize time to YSON" in {
    import spark.implicits._
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

  def prepareWrite(path: String, sortOption: SortOption)
                  (f: ApiServiceTransaction => Unit): Unit = {
    val transaction = YtWrapper.createTransaction(parent = None, timeout = 1 minute)
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
      rows.foreach(r => writer.write(new TestInternalRow(r)))
    } finally {
      try {
        writer.close()
      } finally {
        transaction.commit().join()
      }
    }
  }

  class MockYtOutputWriter(path: YPathEnriched, batchSize: Int, sortOption: SortOption)
    extends YtOutputWriter(
      path,
      schema,
      SparkYtWriteConfiguration(1, batchSize, 5 minutes, typeV3Format = false, distributedWrite = false),
      Map("sort_columns" -> SortColumns.set(sortOption.keys), "unique_keys" -> UniqueKeys.set(sortOption.uniqueKeys))
    ) {
    override protected def initialize(): Unit = {}
  }

  class TestInternalRow(row: Row) extends InternalRow {
    override def numFields: Int = row.length

    override def setNullAt(i: Int): Unit = ???

    override def update(i: Int, value: Any): Unit = ???

    override def copy(): InternalRow = new TestInternalRow(row.copy())

    override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

    override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

    override def getByte(ordinal: Int): Byte = row.getByte(ordinal)

    override def getShort(ordinal: Int): Short = row.getShort(ordinal)

    override def getInt(ordinal: Int): Int = row.getInt(ordinal)

    override def getLong(ordinal: Int): Long = row.getLong(ordinal)

    override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)

    override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)

    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???

    override def getUTF8String(ordinal: Int): UTF8String = UTF8String.fromString(row.getString(ordinal))

    override def getBinary(ordinal: Int): Array[Byte] = ???

    override def getInterval(ordinal: Int): CalendarInterval = ???

    override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???

    override def getArray(ordinal: Int): ArrayData = ???

    override def getMap(ordinal: Int): MapData = ???

    override def get(ordinal: Int, dataType: DataType): AnyRef = ???
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
