package tech.ytsaurus.spyt.format

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}
import tech.ytsaurus.spyt.test.{LocalSpark, SparkMetricsUtils, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtReader, YtWriter}

class YtDynamicFilterTest extends AnyFlatSpec
  with Matchers
  with LocalSpark
  with TmpDir
  with TestUtils
  with YtDistributedReadingTestUtils
  with SparkMetricsUtils {
  behavior of "Runtime filtering for YT DataSource"

  override def reinstantiateSparkSession: Boolean = true

  private val runtimeFiltersConf = Map(
    s"spark.yt.${SparkSettings.Read.KeyColumnsFilterPushdown.Enabled.name}"->"true",
    s"spark.yt.${SparkSettings.Read.YtPartitioningEnabled.name}" -> "true")

  private val FactRowCount = 10000
  private val DimRowCount = 5
  private val TargetYear = 2000

  private val factSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addKey("date_sk", ColumnValueType.INT64)
    .addValue("fact_id", ColumnValueType.INT64)
    .addValue("payload", ColumnValueType.STRING)
    .build()

  private val dimSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("date_sk", ColumnValueType.INT64)
    .addValue("d_year", ColumnValueType.INT64)
    .build()

  private def factData(rowCount: Int = FactRowCount, maxDateSk: Int = DimRowCount): Seq[(Long, Long, String)] = {
    (1 to rowCount).map { i =>
      val dateSk = (i % maxDateSk) + 1
      (dateSk.toLong, i.toLong, s"payload_$i")
    }
  }

  private def dimData(
    dimSize: Int = DimRowCount,
    targetYear: Int = TargetYear,
    rangeStart: Int = 1,
    rangeEnd: Int = 2
  ): Seq[(Long, Long)] = {
    (1 to dimSize).map { sk =>
      val year = if (sk >= rangeStart && sk <= rangeEnd) targetYear else 1990
      (sk.toLong, year.toLong)
    }
  }

  private def createFactTable(
    path: String,
    rowCount: Int = FactRowCount,
    maxDateSk: Int = DimRowCount
  ): Unit = {
    val rows = factData(rowCount, maxDateSk)
      .sortBy(_._1)
      .map { case (dateSk, factId, payload) =>
        s"""{date_sk = $dateSk; fact_id = $factId; payload = "$payload"}"""
      }

    writeTableFromYson(rows, path, factSchema)
  }

  private def createDimTable(
    path: String,
    dimSize: Int = DimRowCount,
    targetYear: Int = TargetYear,
    rangeStart: Int = 1,
    rangeEnd: Int = 2
  ): Unit = {
    val rows = dimData(dimSize, targetYear, rangeStart, rangeEnd)
      .map { case (dateSk, year) =>
        s"""{date_sk = $dateSk; d_year = $year}"""
      }

    writeTableFromYson(rows, path, dimSchema)
  }

  private def withYtTmpDir(testBody: => Unit): Unit = {
    YtWrapper.createDir(tmpPath)
    testBody
  }

  private def joinFilterQuery(factView: String, dimView: String, targetYear: Int = TargetYear): String =
    s"""
       |SELECT COUNT(*) AS cnt
       |FROM $factView
       |JOIN $dimView
       |ON $factView.date_sk = $dimView.date_sk
       |WHERE $dimView.d_year = $targetYear
       |""".stripMargin

  private def expectedJoinCount(
    rowCount: Int = FactRowCount,
    maxDateSk: Int = DimRowCount,
    matchedDateSk: Set[Long] = Set(1L, 2L)
  ): Long = {
    factData(rowCount, maxDateSk).count { case (dateSk, _, _) =>
      matchedDateSk.contains(dateSk)
    }.toLong
  }

  private def collectCount(_spark: SparkSession, query: String): Long = {
    _spark.sql(query).collect().head.getLong(0)
  }

  private def recordsRead(_spark: SparkSession, query: String, runtimeFilteringEnabled: Boolean): Long = {

    _spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", runtimeFilteringEnabled)
      withMetrics(_spark) { listener =>
        _spark.sql(query).collect()
        listener.recordsRead
      }
  }

  private def compareRecordsRead(_spark: SparkSession, query: String): (Long, Long) = {
    val withRuntimeFiltering = recordsRead(_spark, query, runtimeFilteringEnabled = true)
    val withoutRuntimeFiltering = recordsRead(_spark, query, runtimeFilteringEnabled = false)

    withRuntimeFiltering should be > 0L
    withRuntimeFiltering should be < withoutRuntimeFiltering

    (withRuntimeFiltering, withoutRuntimeFiltering)
  }

  Seq(true, false).foreach { distributedReadingEnabled =>
    it should s"reduce records read for sorted fact table using runtime join filter, " +
      s"distributedReadingEnabled=$distributedReadingEnabled" in {
      withSparkSession(runtimeFiltersConf ++ distributedReadingEnabledConf(distributedReadingEnabled)) { _spark =>
        whenSparkVersionAtLeast("3.3.0") {
          withYtTmpDir {
            val factPath = s"$tmpPath/fact_runtime_filter"
            val dimPath = s"$tmpPath/dim_runtime_filter"
            val factView = "yt_fact_runtime_filter"
            val dimView = "yt_dim_runtime_filter"

            createFactTable(factPath)
            createDimTable(dimPath)

            _spark.read.yt(factPath).createOrReplaceTempView(factView)
            _spark.read.yt(dimPath).createOrReplaceTempView(dimView)

            val query = joinFilterQuery(factView, dimView)

            collectCount(_spark, query) shouldBe expectedJoinCount(matchedDateSk = Set(1L, 2L))
            compareRecordsRead(_spark, query)
          }
        }
      }
    }
  }

  testWithDistributedReading("return empty result when runtime join filter contains no matching keys") { distributedReadingEnabled =>
    withSparkSession(runtimeFiltersConf ++ distributedReadingEnabledConf(distributedReadingEnabled)) { _spark =>
      whenSparkVersionAtLeast("3.3.0") {
        withYtTmpDir {
          val factPath = s"$tmpPath/fact_empty_runtime_filter"
          val dimPath = s"$tmpPath/dim_empty_runtime_filter"
          val factView = "yt_fact_empty_runtime_filter"
          val dimView = "yt_dim_empty_runtime_filter"

          createFactTable(factPath)
          createDimTable(dimPath)

          _spark.read.yt(factPath).createOrReplaceTempView(factView)
          _spark.read.yt(dimPath).createOrReplaceTempView(dimView)

          val query = joinFilterQuery(factView, dimView, targetYear = 3000)

          collectCount(_spark, query) shouldBe 0L
          val readRows = recordsRead(_spark, query, runtimeFilteringEnabled = true)
          readRows should be >= 0L
        }
      }
    }
  }
}
