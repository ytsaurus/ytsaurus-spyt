package org.apache.spark.sql.yt

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtReader}

class ReadTransactionStrategyTest extends AnyFlatSpec with Matchers with LocalSpark with TestUtils with TmpDir
  with DynTableTestUtils with YtDistributedReadingTestUtils {
  override def reinstantiateSparkSession: Boolean = true

  testWithDistributedReading("be able to read removed table with spark.yt.read.transactional enabled") { distributedReadingEnabled =>
    withSparkSession(distributedReadingEnabledConf(distributedReadingEnabled)) { _spark =>
      writeTableFromYson(Seq("""{a = 1}"""), tmpPath, TableSchema.builder().addValue("a", ColumnValueType.INT64).build())

      addDeletePathListener(_spark, tmpPath)

      val df = _spark.read.yt(tmpPath)
      df.collect() should contain theSameElementsAs List(Row(1L))
      yt.existsNode(tmpPath).get() shouldBe false
    }
  }

  testWithDistributedReading("be able to read removed table with spark.yt.read.transactional enabled when" +
    " a list of tables is provided") { distributedReadingEnabled =>
    withSparkSession(distributedReadingEnabledConf(distributedReadingEnabled)) { _spark =>
      createSampleTables(tmpPath)
      addDeletePathListener(_spark, s"$tmpPath/table2")

      val df = _spark.read.yt(s"$tmpPath/table1", s"$tmpPath/table2")

      df.collect() should contain theSameElementsAs Seq(
        Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d")
      )
    }
  }

  testWithDistributedReading("support dataframe reusing") { distributedReadingEnabled =>
    withSparkSession(distributedReadingEnabledConf(distributedReadingEnabled)) { _spark =>
      createSampleTables(tmpPath)

      val df = _spark.read.yt(s"$tmpPath/table1").cache()
      val result = Seq(Row(1L, "a"), Row(2L, "b"))

      // First pass
      df.collect() should contain theSameElementsAs result
      // Second pass
      df.collect() should contain theSameElementsAs result
    }
  }

  List(false, true).foreach { transactional =>
    testWithDistributedReading(s"read nested tables with spark.yt.read.transactional = $transactional") { distributedReadingEnabled =>
      withSparkSession(Map(
        "spark.yt.read.transactional" -> transactional.toString,
        distributedReadingEnabledConf(distributedReadingEnabled).head
      )) { _spark =>
        val dirPath = s"$tmpPath/root_directory"
        createSampleTables(dirPath)

        val df = _spark.read.yt(dirPath)
        df.collect() should contain theSameElementsAs Seq(
          Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f"), Row(7L, "g"), Row(8L, "h")
        )
      }
    }
  }

  private def createSampleTables(dirPath: String): Unit = {
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
  }

  private def addDeletePathListener(_spark: SparkSession, path: String): Unit = {
    _spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        if (taskStart.taskInfo.index == 0) {
          yt.removeNode(path).join()
        }
      }
    })
  }
}
