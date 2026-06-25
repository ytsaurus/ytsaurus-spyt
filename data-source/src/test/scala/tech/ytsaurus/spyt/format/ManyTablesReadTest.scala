package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import tech.ytsaurus.client.request.PartitionTables
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, CountDownLatch, CompletionException, Executors, TimeUnit}

class ManyTablesReadTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir
  with TestUtils with YtDistributedReadingTestUtils with TimeLimits {

  behavior of "spark.read.yt with many tables"

  implicit val defaultSignaler: Signaler = ThreadSignaler

  private val tablesCount = 100
  private val rowsPerTable = 5
  private val readTimeout = 10.seconds
  private val maxConcurrency = 4

  private val tableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("table_id", ColumnValueType.INT64)
    .addValue("row_idx", ColumnValueType.INT64)
    .build()

  private val tablesDir = s"$testDir/tables"
  private var tablePaths: Seq[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    YtWrapper.createDir(tablesDir)
    tablePaths = writeManyTables(tablesDir)
  }

  private def writeManyTables(parentPath: String): Seq[String] = {
    val paths = (0 until tablesCount).map(i => s"$parentPath/t_$i")

    val parallelism = math.min(tablesCount, 8)
    val executor = Executors.newFixedThreadPool(parallelism)

    try {
      val futures = paths.zipWithIndex.map { case (path, tableIdx) =>
        val rows = (0 until rowsPerTable).map(j => s"{table_id = $tableIdx; row_idx = $j}")
        CompletableFuture.runAsync(
          () => {
            writeTableFromYson(rows, path, tableSchema)
          },
          executor
        )
      }

      try {
        CompletableFuture.allOf(futures: _*).join()
      } catch {
        case e: CompletionException if e.getCause != null =>
          throw e.getCause
      }
      paths
    } finally {
      executor.shutdown()
    }
  }

  private val expectedRows: Set[Row] = (for {
    i <- 0 until tablesCount
    j <- 0 until rowsPerTable
  } yield Row(i.toLong, j.toLong)).toSet

  Seq(
    ("as path list", () => tablePaths),
    ("as directory", () => Seq(tablesDir))
  ).foreach { case (label, pathsProvider) =>
    it should s"read 100+ tables $label" in withConfs(
      Map("spark.yt.read.ytDistributedReading.enabled" -> "true",
      "spark.sql.sources.parallelPartitionDiscovery.threshold" -> "1024")
    ) {

      val df = spark.read.yt(pathsProvider(): _*)

      failAfter(readTimeout) {
        val count = df.count()
        count shouldBe tablesCount * rowsPerTable
      }

      failAfter(readTimeout) {
        val rows = df.collect().map(r => Row(r.getLong(0), r.getLong(1))).toSet
        rows shouldBe expectedRows
      }
    }
  }

  it should "bound the number of concurrent partitionTables requests" in withConfs(
    Map("spark.yt.read.ytDistributedReading.enabled" -> "true",
      "spark.ytsaurus.throttling.maxConcurrency" -> maxConcurrency.toString,
      "spark.sql.sources.parallelPartitionDiscovery.threshold" -> "1024")
  ) {
    val inFlight = new AtomicInteger(0)
    val maxInFlight = new AtomicInteger(0)

    val allSlotsOccupied = new CountDownLatch(maxConcurrency)
    val release = new CompletableFuture[Void]()

    withSpyYt(spark, Some(StatisticsReporter)) { spyYt =>
      Mockito.doAnswer { invocation =>
        val current = inFlight.incrementAndGet()

        maxInFlight.accumulateAndGet(current, Math.max)
        allSlotsOccupied.countDown()

        val realFuture = invocation
          .callRealMethod()
          .asInstanceOf[CompletableFuture[AnyRef]]

        realFuture
          .thenCompose { value =>
            release.thenApply[AnyRef](_ => value)
          }
          .whenComplete { (_, _) =>
            inFlight.decrementAndGet()
          }
      }.when(spyYt).partitionTables(ArgumentMatchers.any(classOf[PartitionTables]))

      val readFuture = CompletableFuture.runAsync { () =>
        val df = spark.read.yt(tablesDir)

        df.count() shouldBe tablesCount * rowsPerTable

        df.collect()
          .map(r => Row(r.getLong(0), r.getLong(1)))
          .toSet shouldBe expectedRows
      }

      try {
        allSlotsOccupied.await(30, TimeUnit.SECONDS) shouldBe true
      } finally {
        release.complete(null)
      }

      readFuture.get(60, TimeUnit.SECONDS)

      maxInFlight.get() shouldBe maxConcurrency
    }
  }
}
