package org.apache.spark.sql.yt

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

class ReadTransactionStrategyTest extends AnyFlatSpec with Matchers with LocalSpark with TestUtils with TmpDir with DynTableTestUtils {
  override def reinstantiateSparkSession: Boolean = true

  it should "be able to read removed table with spark.yt.read.transactional enabled" in withSparkSession() { _spark =>
    writeTableFromYson(Seq("""{a = 1}"""), tmpPath, TableSchema.builder().addValue("a", ColumnValueType.INT64).build())

    _spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        if (taskStart.taskInfo.index == 0) {
          yt.removeNode(tmpPath).join()
        }
      }
    })

    val df = _spark.read.yt(tmpPath)
    df.collect() should contain theSameElementsAs List(Row(1L))
    yt.existsNode(tmpPath).get() shouldBe false
  }

  List(false, true).foreach { transactional =>
    it should s"read nested tables with spark.yt.read.transactional = $transactional" in withSparkSession(Map(
      "spark.yt.read.transactional" -> transactional.toString
    )) { _spark =>
      val dirPath = s"$tmpPath/root_directory"
      val innerDirPath = s"$dirPath/inner"
      YtWrapper.createDir(innerDirPath)

      val schema = TableSchema.builder()
        .addValue("a", ColumnValueType.INT64)
        .addValue("b", ColumnValueType.STRING)
        .build()

      val data = Map(
        s"$dirPath/table1" -> Seq("""{a = 1; b = "a"}""", """{a = 2; b = "b"}"""),
        s"$dirPath/table2" -> Seq("""{a = 3; b = "c"}""", """{a = 4; b = "d"}"""),
        s"$innerDirPath/table3" -> Seq("""{a = 5; b = "e"}""", """{a = 6; b = "f"}"""),
        s"$innerDirPath/table4" -> Seq("""{a = 7; b = "g"}""", """{a = 8; b = "h"}""")
      )
      data.foreach { case (path, rows) =>
        writeTableFromYson(rows, path, schema)
      }

      val df = _spark.read.yt(dirPath)
      df.collect() should contain theSameElementsAs Seq(
        Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f"), Row(7L, "g"), Row(8L, "h")
      )
    }
  }
}
