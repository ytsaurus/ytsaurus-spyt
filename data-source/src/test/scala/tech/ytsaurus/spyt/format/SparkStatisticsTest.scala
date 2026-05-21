package tech.ytsaurus.spyt.format

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test.{LocalSpark, SparkMetricsUtils, TestUtils, TmpDir}
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtReader, YtWriter}
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}

class SparkStatisticsTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with YtDistributedReadingTestUtils with SparkMetricsUtils {

  behavior of "Spark statistics"

  import spark.implicits._

  testWithDistributedReading("correctly calculate bytes written for simple write"){ _ =>
    val data = (0 until 100).map(x => (x, x * 2, x * 3))
    val df = data.toDF("col1", "col2", "col3")

    withMetrics(spark) { listener =>
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

    withMetrics(spark) { listener =>
      val count = spark.read.yt(tmpPath).count()

      count shouldBe 100L
      listener.recordsRead shouldBe 100L
    }
  }

  testWithDistributedReading("correctly account only processed data after filter"){ _ =>
    whenSparkVersionAtLeast("3.3.0") {
      val data = (0 until 1000).map(x => (x, x % 2 == 0))
      val df = data.toDF("id", "is_even")

      withMetrics(spark) { writeListener =>
        df.write.yt(tmpPath)

        writeListener.bytesWritten should be > 0L
        writeListener.recordsWritten shouldBe 1000L
      }

      withMetrics(spark) { readListener =>
        val filteredDf = spark.read.yt(tmpPath).filter("is_even")
        val count = filteredDf.count()

        count shouldBe 500L
        readListener.bytesRead should be > 0L
        readListener.recordsRead shouldBe 1000L
      }
    }
  }

  testWithDistributedReading("track metrics for read-write pipeline"){ _ =>
    whenSparkVersionAtLeast("3.3.0") {
      val sourceData = (0 until 200).map(x => (x, x * 2, x * 3))
      val sourceDf = sourceData.toDF("col1", "col2", "col3")
      sourceDf.write.yt(s"$tmpPath/source")

      withMetrics(spark) { listener =>
        val transformedDf = spark.read.yt(s"$tmpPath/source")
          .filter($"col1" < 100)

        transformedDf.write.yt(s"$tmpPath/target")

        listener.bytesRead should be > 0L
        listener.recordsRead shouldBe 200L

        listener.bytesWritten should be > 0L
        listener.recordsWritten shouldBe 100L
      }
    }
  }

  testWithDistributedReading("pushdown filters reduce data weight") { _ =>
    whenSparkVersionAtLeast("3.3.0") {
      (1L to 1000L).map(x => (x, x % 2, s"string_$x")).toDF("a", "b", "text")
        .repartition(2).sort("a", "b").write.sortedBy("a", "b").yt(tmpPath)

      val bytes = Seq(false, true).map { pushdown =>
        withConfs(Map(s"spark.yt.${SparkSettings.Read.KeyColumnsFilterPushdown.Enabled.name}" -> pushdown.toString)) {
          withMetrics(spark)(l => {
            spark.read.yt(tmpPath).filter("a >= 100 AND a <= 200 AND b == 0").count()
            l.bytesRead
          })
        }
      }

      bytes(1) should (be > 0L and be < bytes.head)
    }
  }

  testWithDistributedReading("track metrics for join operations"){ _ =>
    whenSparkVersionAtLeast("3.3.0") {
      val data1 = (0 until 100).map(x => (x, x * 2)).toDF("id", "val1")
      val data2 = (0 until 100).map(x => (x, x * 3)).toDF("id", "val2")

      data1.write.yt(s"$tmpPath/table1")
      data2.write.yt(s"$tmpPath/table2")

      withMetrics(spark) { listener =>
        val df1 = spark.read.yt(s"$tmpPath/table1")
        val df2 = spark.read.yt(s"$tmpPath/table2")
        val joinedDf = df1.join(df2, "id")

        val count = joinedDf.count()

        count shouldBe 100L
        listener.bytesRead should be > 0L
        listener.recordsRead shouldBe 200L
      }
    }
  }

  testWithDistributedReading("track metrics for aggregation operations"){ _ =>
    whenSparkVersionAtLeast("3.3.0") {
      val data = (0 until 1000).map(x => (x % 10, x))
      val df = data.toDF("group_id", "value")

      df.write.yt(tmpPath)

      withMetrics(spark) { listener =>
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
}
