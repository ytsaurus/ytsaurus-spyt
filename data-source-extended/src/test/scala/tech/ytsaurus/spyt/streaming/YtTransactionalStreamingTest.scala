package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchersSugar.{any, eqTo}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Streaming
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.InconsistentReadEnabled
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.time.Duration
import scala.collection.SortedMap
import scala.util.Success

class YtTransactionalStreamingTest extends AnyFlatSpec with Matchers with MockitoSugar with LocalYtClient
  with LocalSpark with TmpDir with DynTableTestUtils with QueueTestUtils {

  import tech.ytsaurus.spyt._

  private val consumerPath = "//tmp/path/to/consumer"
  private val queuePath = "//tmp/path/to/queue"
  private val cluster = YtWrapper.clusterName()

  private def createOffsetFixture(lastOffset: Long = 50L, newOffset: Long = 60L): OffsetFixture = {
    OffsetFixture(
      lastCommitted = YtQueueOffset(cluster, queuePath, SortedMap(0 -> lastOffset)),
      newCommit = YtQueueOffset(cluster, queuePath, SortedMap(0 -> newOffset))
    )
  }

  private def createMockOffsetProvider(offsets: OffsetFixture, advanceThrows: Option[Throwable] = None): YtQueueOffsetProvider = {
    val mockProvider = mock[YtQueueOffsetProvider]
    when(mockProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(offsets.lastCommitted)
    advanceThrows.foreach { exception =>
      when(
        mockProvider.advance(
          any[String], any[YtQueueOffset], any[YtQueueOffset], any[Option[YtQueueOffset]], any[Option[String]]
        )(any[CompoundClient])
      )
        .thenThrow(exception)
    }
    mockProvider
  }

  private def createStreamingSource(schema: StructType = new StructType(), offsetProvider: YtQueueOffsetProvider): YtStreamingSource = {
    new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, schema, Map.empty, offsetProvider = offsetProvider)
  }

  private def withTransactionContext[T](txId: String)(block: => T): T = {
    YtStreamingTransactionContext.setTransactionId(txId)
    try {
      block
    } finally {
      YtStreamingTransactionContext.clearTransactionId()
    }
  }

  private def withTransactionFixture[T](block: TransactionFixture => T): T = {
    val fixture = createTransactionFixture()
    try {
      block(fixture)
    } catch {
      case e: Throwable =>
        fixture.abortSafely()
        throw e
    }
  }

  private def createTransactionFixture(): TransactionFixture = {
    val transaction = YtWrapper.createTransaction(None, Duration.ofMinutes(5), sticky = true)
    TransactionFixture(transaction, transaction.getId.toString)
  }

  private def verifyAdvanceCalled(mockProvider: YtQueueOffsetProvider, offsets: OffsetFixture,
    expectedTxId: Option[String]): Unit = {
    verify(mockProvider, times(1)).advance(
      eqTo(consumerPath),
      eqTo(offsets.newCommit),
      eqTo(offsets.lastCommitted),
      eqTo(None),
      eqTo(expectedTxId)
    )(any[CompoundClient])
  }

  private def prepareTestTable(): String = {
    YtWrapper.createDir(tmpPath)
    val resultPath = s"$tmpPath/result-${java.util.UUID.randomUUID()}"
    prepareOrderedTestTable(resultPath, orderedTestSchema, enableDynamicStoreRead = true)
    resultPath
  }

  private def readTableData(path: String): Array[Row] = {
    spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(path)
      .select("a", "b", "c")
      .orderBy("a")
      .collect()
  }

  private def readTableCount(path: String): Long = {
    spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(path)
      .count()
  }

  case class OffsetFixture(lastCommitted: YtQueueOffset, newCommit: YtQueueOffset)

  case class TransactionFixture(transaction: ApiServiceTransaction, txId: String) {
    def commitAndVerify(): Unit = transaction.commit().join()

    def abortSafely(): Unit = transaction.abort().join()
  }

  behavior of "YtStreamingSource transactional commit"

  it should "pass parent transaction id to provider's advance on commit" in {
    val offsets = createOffsetFixture()
    val mockOffsetProvider = createMockOffsetProvider(offsets)
    val source = createStreamingSource(offsetProvider = mockOffsetProvider)
    val parentTxId = "test-transaction-id"

    withTransactionContext(parentTxId) {
      source.commit(offsets.newCommit)
      verifyAdvanceCalled(mockOffsetProvider, offsets, Some(parentTxId))
    }
  }

  it should "propagate exception from provider's advance on transactional commit" in {
    val offsets = createOffsetFixture()
    val expectedException = new RuntimeException("advanceConsumer failed in transaction")
    val mockOffsetProvider = createMockOffsetProvider(offsets, Some(expectedException))
    val source = createStreamingSource(offsetProvider = mockOffsetProvider)
    val parentTxId = "test-transaction-id"

    withTransactionContext(parentTxId) {
      val exception = intercept[RuntimeException] {
        source.commit(offsets.newCommit)
      }
      exception.getMessage shouldBe "advanceConsumer failed in transaction"
    }
  }

  behavior of "Transactional streaming commit"

  it should "commit data successfully with transactional streaming" in {
    val resultPath = prepareTestTable()

    withTransactionFixture { fixture =>
      withTransactionContext(fixture.txId) {
        val sink = new YtStreamingSink(spark.sqlContext, resultPath, Map.empty)
        val testData = getTestData(0, 9)
        val sourceData = spark.createDataFrame(testData)

        sink.addBatch(0, sourceData)

        val offsets = createOffsetFixture(lastOffset = -1L, newOffset = 9L)
        val mockOffsetProvider = createMockOffsetProvider(offsets)
        val source = createStreamingSource(sourceData.schema, mockOffsetProvider)

        source.commit(offsets.newCommit)

        verifyAdvanceCalled(mockOffsetProvider, offsets, Some(fixture.txId))

        fixture.commitAndVerify()

        readTableCount(resultPath) shouldBe 10L

        val resultData = readTableData(resultPath)
        val expectedData = spark.createDataFrame(testData).collect()
        resultData should contain theSameElementsAs expectedData
      }
    }
  }

  behavior of "YtStreamingSource transactional latestOffset"

  it should "refresh lastCommittedOffset on every latestOffset call in transactional mode " +
    "even without max_rows_per_partition" in {
    val mockProvider = mock[YtQueueOffsetProvider]
    val initialOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> -1L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    when(mockProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(initialOffset)
    when(mockProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Success(maxOffset))

    withConf(Streaming.Transactional, true) {
      val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath,
        new StructType(), Map.empty, offsetProvider = mockProvider)
      reset(mockProvider)
      when(mockProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
        .thenReturn(initialOffset)
      when(mockProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
        .thenReturn(Success(maxOffset))

      val firstResult = source.latestOffset(null, ReadLimit.allAvailable())
      val secondResult = source.latestOffset(null, ReadLimit.allAvailable())

      firstResult shouldBe maxOffset
      secondResult shouldBe maxOffset
      verify(mockProvider, times(2)).getCurrentOffset(
        eqTo(cluster), eqTo(consumerPath), eqTo(queuePath))(any[CompoundClient])
    }
  }

  it should "still compute correct end offset when max_rows_per_partition is set " +
    "(transactional, no regression)" in {
    val mockProvider = mock[YtQueueOffsetProvider]
    val initialOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 5L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    when(mockProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(initialOffset)
    when(mockProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Success(maxOffset))

    withConf(Streaming.Transactional, true) {
      val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath,
        new StructType(), Map.empty, offsetProvider = mockProvider)

      val result = source.latestOffset(null, ReadLimit.maxRows(10L)).asInstanceOf[YtQueueOffset]

      result.partitions(0) shouldBe 15L
    }
  }

  it should "not move lastCommittedOffset backwards when YT consumer returns a stale or empty value" in {
    val mockProvider = mock[YtQueueOffsetProvider]
    val initialOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 25L))
    val staleOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> -1L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    when(mockProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(initialOffset, staleOffset)
    when(mockProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Success(maxOffset))

    withConf(Streaming.Transactional, true) {
      val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath,
        new StructType(), Map.empty, offsetProvider = mockProvider)

      val result = source.latestOffset(null, ReadLimit.maxRows(10L)).asInstanceOf[YtQueueOffset]

      result.partitions(0) shouldBe math.min(25L + 10L, 50L)
    }
  }

  behavior of "Transactional streaming rollback"

  it should "rollback data when commit fails with transactional streaming" in {
    val resultPath = prepareTestTable()

    withTransactionFixture { fixture =>
      withTransactionContext(fixture.txId) {
        val sink = new YtStreamingSink(spark.sqlContext, resultPath, Map.empty)
        val testData = getTestData(0, 9)
        val sourceData = spark.createDataFrame(testData)

        sink.addBatch(0, sourceData)

        val offsets = createOffsetFixture(lastOffset = -1L, newOffset = 9L)
        val expectedException = new RuntimeException("advanceConsumer failed - simulated error")
        val mockOffsetProvider = createMockOffsetProvider(offsets, Some(expectedException))
        val source = createStreamingSource(sourceData.schema, mockOffsetProvider)

        intercept[RuntimeException] {
          source.commit(offsets.newCommit)
        }

        fixture.abortSafely()

        readTableCount(resultPath) shouldBe 0L
      }
    }
  }
}
