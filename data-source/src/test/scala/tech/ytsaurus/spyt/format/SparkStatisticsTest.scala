package tech.ytsaurus.spyt.format

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtReader, YtWriter}
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}

class SparkStatisticsTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with YtDistributedReadingTestUtils {

  behavior of "Spark statistics"

  import spark.implicits._

  object MetricsListener {
    class BytesMetricsListener extends SparkListener {
      var bytesWritten: Long = 0L
      var bytesRead: Long = 0L
      var recordsWritten: Long = 0L
      var recordsRead: Long = 0L

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd.taskMetrics != null) {
          bytesWritten += taskEnd.taskMetrics.outputMetrics.bytesWritten
          recordsWritten += taskEnd.taskMetrics.outputMetrics.recordsWritten
          bytesRead += taskEnd.taskMetrics.inputMetrics.bytesRead
          recordsRead += taskEnd.taskMetrics.inputMetrics.recordsRead
        }
      }

      def reset(): Unit = {
        bytesWritten = 0L
        bytesRead = 0L
        recordsWritten = 0L
        recordsRead = 0L
      }
    }

    def withMetrics[T](spark: org.apache.spark.sql.SparkSession)(action: BytesMetricsListener => T): T = {
      val listener = new BytesMetricsListener()
      spark.sparkContext.addSparkListener(listener)
      try {
        val result = action(listener)
        result
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }

  testWithDistributedReading("correctly calculate bytes written for simple write"){ _ =>
    val data = (0 until 100).map(x => (x, x * 2, x * 3))
    val df = data.toDF("col1", "col2", "col3")

    MetricsListener.withMetrics(spark) { listener =>
      df.write.yt(tmpPath)

      listener.bytesWritten should be > 0L
      listener.recordsWritten shouldBe 100L

      val resultDf = spark.read.yt(tmpPath)
      resultDf.count() shouldBe 100L
    }
  }

  testWithDistributedReading("correctly calculate bytes read for simple read"){ _ =>
    val data = (0 until 100).map(x => (x, x * 2, x * 3))
    val df = data.toDF("col1", "col2", "col3")
    df.write.yt(tmpPath)

    MetricsListener.withMetrics(spark) { listener =>
      val count = spark.read.yt(tmpPath).count()

      count shouldBe 100L
      listener.recordsRead shouldBe 100L
    }
  }

  testWithDistributedReading("correctly account only processed data after filter"){ _ =>
    val data = (0 until 1000).map(x => (x, x % 2 == 0))
    val df = data.toDF("id", "is_even")

    MetricsListener.withMetrics(spark) { writeListener =>
      df.write.yt(tmpPath)

      writeListener.bytesWritten should be > 0L
      writeListener.recordsWritten shouldBe 1000L
    }

    MetricsListener.withMetrics(spark) { readListener =>
      val filteredDf = spark.read.yt(tmpPath).filter("is_even")
      val count = filteredDf.count()

      count shouldBe 500L
      readListener.bytesRead should be > 0L
      readListener.recordsRead shouldBe 1000L
    }
  }

  testWithDistributedReading("track metrics for read-write pipeline"){ _ =>
    val sourceData = (0 until 200).map(x => (x, x * 2, x * 3))
    val sourceDf = sourceData.toDF("col1", "col2", "col3")
    sourceDf.write.yt(s"$tmpPath/source")

    MetricsListener.withMetrics(spark) { listener =>
      val transformedDf = spark.read.yt(s"$tmpPath/source")
        .filter($"col1" < 100)

      transformedDf.write.yt(s"$tmpPath/target")

      listener.bytesRead should be > 0L
      listener.recordsRead shouldBe 200L

      listener.bytesWritten should be > 0L
      listener.recordsWritten shouldBe 100L
    }
  }

  testWithDistributedReading("pushdown filters reduce data weight") { _ =>
    cancel("SPYT-996: Waiting for proper input metrics support in YT side for tablePartitionReader")
    (1L to 1000L).map(x => (x, x % 2, s"string_$x")).toDF("a", "b", "text")
      .repartition(2).sort("a", "b").write.sortedBy("a", "b").yt(tmpPath)

    val bytes = Seq(false, true).map { pushdown =>
      withConfs(Map(s"spark.yt.${SparkSettings.Read.KeyColumnsFilterPushdown.Enabled.name}" -> pushdown.toString)) {
        MetricsListener.withMetrics(spark)(l => {
          spark.read.yt(tmpPath).filter("a >= 100 AND a <= 200 AND b == 0").count()
          l.bytesRead
        })
      }
    }

    bytes(1) should (be > 0L and be < bytes(0))
  }


  testWithDistributedReading("compare read and write bytes for same data"){ _ =>
    cancel("SPYT-996: Waiting for proper input metrics support in YT side for tablePartitionReader")
    val data = (0 until 500).map(x => (x, x * 2, x * 3))
    val df = data.toDF("col1", "col2", "col3")

    var writtenBytes = 0L
    var readBytes = 0L

    MetricsListener.withMetrics(spark) { listener =>
      df.write.yt(tmpPath)
      writtenBytes = listener.bytesWritten
    }

    MetricsListener.withMetrics(spark) { listener =>
      val result = spark.read.yt(tmpPath).collect()
      result.length shouldBe 500L
      readBytes = listener.bytesRead
    }

    writtenBytes should be > 0L
    readBytes should be > 0L

    readBytes should be >= (writtenBytes * 0.8).toLong
    readBytes should be <= (writtenBytes * 1.2).toLong
  }

  testWithDistributedReading("track metrics for join operations"){ _ =>
    val data1 = (0 until 100).map(x => (x, x * 2)).toDF("id", "val1")
    val data2 = (0 until 100).map(x => (x, x * 3)).toDF("id", "val2")

    data1.write.yt(s"$tmpPath/table1")
    data2.write.yt(s"$tmpPath/table2")

    MetricsListener.withMetrics(spark) { listener =>
      val df1 = spark.read.yt(s"$tmpPath/table1")
      val df2 = spark.read.yt(s"$tmpPath/table2")
      val joinedDf = df1.join(df2, "id")

      val count = joinedDf.count()

      count shouldBe 100L
      listener.bytesRead should be > 0L
      listener.recordsRead shouldBe 200L
    }
  }

  testWithDistributedReading("track metrics for aggregation operations"){ _ =>
    val data = (0 until 1000).map(x => (x % 10, x))
    val df = data.toDF("group_id", "value")

    df.write.yt(tmpPath)

    MetricsListener.withMetrics(spark) { listener =>
      val aggregatedDf = spark.read.yt(tmpPath)
        .groupBy("group_id")
        .count()

      val count = aggregatedDf.count()

      count shouldBe 10L
      listener.bytesRead should be > 0L
      listener.recordsRead shouldBe 1000L
    }
  }
}
