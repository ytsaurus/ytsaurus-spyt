package tech.ytsaurus.spyt.format.batch

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.serializers.WriteSchemaConverter
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.{OptimizeMode, YtArrowInputStream, YtReadContext, YtReadSettings}
import tech.ytsaurus.spyt.{SchemaTestUtils, YtDistributedReadingTestUtils, YtReader, YtWriter}

import java.io.InputStream
import scala.collection.mutable.ArrayBuffer

class ArrowBatchReaderTest extends AnyFlatSpec with Matchers with TmpDir with SchemaTestUtils
  with LocalSpark with ReadBatchRows with YtDistributedReadingTestUtils {

  behavior of "ArrowBatchReader"

  private val schema = StructType(Seq(
    structField("_0", DoubleType),
    structField("_1", DoubleType),
    structField("_2", DoubleType)
  ))

  it should "read old arrow format (< 0.15.0)" in {
    val stream = new TestInputStream(getClass.getResourceAsStream("arrow_old"))
    val reader = new ArrowBatchReader(stream, schema, new WriteSchemaConverter().tableSchema(schema))
    val expected = readExpected("arrow_old_expected", schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs expected
  }

  it should "read new arrow format (>= 0.15.0)" in {
    val stream = new TestInputStream(getClass.getResourceAsStream("arrow_new"))
    val reader = new ArrowBatchReader(stream, schema, new WriteSchemaConverter().tableSchema(schema))
    val expected = readExpected("arrow_new_expected", schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs expected
  }

  testWithDistributedReading("read empty stream") { distributedReadingEnabled =>
    import spark.implicits._
    val schema = StructType(Seq(structField("a", IntegerType)))
    val data = Seq[Int]()
    val df = data.toDF("a")

    df.write.optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    if (distributedReadingEnabled) {
      val delegates = getDelegatesForTable(spark, tmpPath)
      delegates.size shouldEqual 0
    } else {
      implicit val ytReadContext: YtReadContext =
        YtReadContext(yt, YtReadSettings.default.copy(distributedReadingEnabled = distributedReadingEnabled))
      val stream = YtWrapper.readTableArrowStream(YPath.simple(tmpPath))
      val reader = new ArrowBatchReader(stream, schema, new WriteSchemaConverter().tableSchema(schema))
      val rows = readFully(reader, schema, Int.MaxValue)
      rows should contain theSameElementsAs data.map(Row(_))
    }
  }

  testWithDistributedReading("count table") { distributedReadingEnabled =>
    import spark.implicits._

    val count = 500
    val data = (0 until count).map(x => (x / 100, x / 10)).toDF("a", "b")

    Seq("scan", "lookup").foreach { optimizeMode =>
      data.write.mode(SaveMode.Overwrite).optimizeFor(optimizeMode).yt(tmpPath)
      val res = spark.read.enableArrow.yt(tmpPath).count()
      res shouldBe count
    }
  }

  testWithDistributedReading("read arrow stream from yt") { distributedReadingEnabled =>
    import spark.implicits._
    val schema = StructType(Seq(structField("a", IntegerType)))

    val ytReadSettings =  YtReadSettings.default.copy(distributedReadingEnabled = distributedReadingEnabled)
    implicit val ytReadContext: YtReadContext = YtReadContext(yt, ytReadSettings)

    def testSlice(data: Seq[Int], batchSize: Int, lowerRowIndex: Int, upperRowIndex: Int): Unit = {
      if (distributedReadingEnabled) {
        val mtps = YtWrapper.partitionTables(
          YPath.simple(tmpPath),
          spark.sessionState.conf.filesMaxPartitionBytes,
          enableCookies = true
        )
        val cookies = mtps.map(mtp => mtp.getCookie)
        val allRows: ArrayBuffer[Row] = ArrayBuffer.empty[Row]
        for (cookie <- cookies) {
          val stream = YtWrapper.createTablePartitionArrowStream(cookie)
          val reader = new ArrowBatchReader(stream, schema, new WriteSchemaConverter().tableSchema(schema))
          val rows = readFully(reader, schema, batchSize)
          allRows ++= rows.map(row => row.getInt(0)).map(Row(_))
        }
        val filteredRows = allRows.slice(lowerRowIndex, upperRowIndex)
        val expected = data.slice(lowerRowIndex, upperRowIndex).map(Row(_))
        filteredRows shouldEqual expected
      } else {
        val stream = YtWrapper.readTableArrowStream(YPath.simple(tmpPath).withRange(lowerRowIndex, upperRowIndex))
        val reader = new ArrowBatchReader(stream, schema, new WriteSchemaConverter().tableSchema(schema))
        val rows = readFully(reader, schema, batchSize)
        val expected = data.slice(lowerRowIndex, upperRowIndex).map(Row(_))
        rows shouldEqual expected
      }
    }

    val chunkCount = 3
    val chunkRowCounts = List(1, 5, 10)

    chunkRowCounts.foreach { chunkRowCount =>
        val data = (0 to chunkCount * chunkRowCount).toList
        YtWrapper.remove(tmpPath, force = true)

        (0 to chunkCount).foreach { chunkIndex =>
            val chunk = data.slice(chunkIndex * chunkRowCount, (chunkIndex + 1) * chunkRowCount).toDF("a")
            chunk.write.optimizeFor(OptimizeMode.Scan).sortedBy("a").mode(SaveMode.Append).yt(tmpPath)
        }

        testSlice(data, chunkRowCount, 0, 10)
        testSlice(data, chunkRowCount, 18, 2)
        testSlice(data, chunkRowCount, 6, 6)
    }
  }

  private def readExpected(filename: String, schema: StructType): Seq[Row] = {
    val path = getClass.getResource(filename).getPath
    spark.read.schema(schema).csv(s"file://$path").collect()
  }

  private class TestInputStream(is: InputStream) extends YtArrowInputStream {
    override def isNextPage: Boolean = false

    override def isEmptyPage: Boolean = false

    override def read(): Int = is.read()

    override def read(b: Array[Byte]): Int = is.read(b)

    override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
  }
}
