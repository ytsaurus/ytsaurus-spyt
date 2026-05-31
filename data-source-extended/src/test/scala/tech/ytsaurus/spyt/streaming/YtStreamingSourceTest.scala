package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.mockito.ArgumentMatchersSugar.{any, eqTo}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Streaming
import tech.ytsaurus.spyt.test.{LocalSpark, LocalYtClient}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.collection.SortedMap
import scala.util.{Failure, Success}

class YtStreamingSourceTest extends AnyFlatSpec with Matchers with MockitoSugar with LocalYtClient with LocalSpark {

  override def afterEach(): Unit = {
    try {
      YtStreamingTransactionContext.clearRecoveryNeeded()
    } finally {
      super.afterEach()
    }
  }

  private val consumerPath = "//tmp/path/to/consumer"
  private val queuePath = "//tmp/path/to/queue"
  private val cluster = YtWrapper.clusterName()

  private def singlePartitionOffset(value: Long): YtQueueOffset = {
    YtQueueOffset(cluster, queuePath, SortedMap(0 -> value))
  }

  private def offsetProviderWith(
    currentOffset: YtQueueOffset,
    maxOffset: YtQueueOffset): YtQueueOffsetProvider = {
    val offsetProvider = mock[YtQueueOffsetProvider]
    when(offsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(currentOffset)
    when(offsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))
    offsetProvider
  }

  private def sourceWith(
    offsetProvider: YtQueueOffsetProvider,
    parameters: Map[String, String] = Map.empty,
    schema: StructType = new StructType(),
    sqlContext: SQLContext = spark.sqlContext): YtStreamingSource = {
    new YtStreamingSource(sqlContext, consumerPath, queuePath, schema, parameters, offsetProvider = offsetProvider)
  }

  behavior of "YtStreamingSource"

  it should "use provider for current offset in latestOffset if start is not defined" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))
    val currentOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))

    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))
    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(currentOffset)

    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(),
      Map("max_rows_per_partition" -> "10"), offsetProvider = mockOffsetProvider
    )

    val limit = ReadLimit.maxRows(10)
    val resultOffset = source.latestOffset(null, limit).asInstanceOf[YtQueueOffset]

    val expectedPartitionOffset = currentOffset.partitions(0) + 10L
    resultOffset.partitions(0) shouldBe expectedPartitionOffset
  }

  it should "return cached offset on provider failure" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]
    val initialMaxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))
    val lastCommittedOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))

    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Success(initialMaxOffset), Failure(new RuntimeException("YT is temporarily unavailable")))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(lastCommittedOffset)

    val source = new YtStreamingSource(
      sqlContext,
      consumerPath,
      queuePath,
      new StructType(),
      parameters = Map("max_rows_per_partition" -> "10"),
      offsetProvider = mockOffsetProvider
    )

    val cachedAfterSuccess = source.getMaxOffset
    cachedAfterSuccess shouldBe Some(initialMaxOffset)

    val cachedAfterProviderFailure = source.getMaxOffset
    cachedAfterProviderFailure shouldBe Some(initialMaxOffset)
    verify(mockOffsetProvider, times(2)).getMaxOffset(eqTo(cluster), eqTo(queuePath))(any[CompoundClient])
  }

  it should "(transactional mode) use consumer lower bound in latestOffset when Spark checkpoint is ahead of consumer" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val staleConsumerOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 30L))
    val freshConsumerOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(staleConsumerOffset, freshConsumerOffset)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, new StructType(),
      Map("max_rows_per_partition" -> "10"), offsetProvider = mockOffsetProvider)

    val sparkCachedStart = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 80L))

    withConf(Streaming.Transactional, true) {
      val resultOffset = source.latestOffset(sparkCachedStart, ReadLimit.maxRows(10)).asInstanceOf[YtQueueOffset]
      resultOffset.partitions shouldBe SortedMap(0 -> (freshConsumerOffset.partitions(0) + 10L))
    }
  }

  it should "(transactional mode) use consumer lower bound in getBatch when Spark start is ahead of consumer" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val staleConsumerOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 30L))
    val freshConsumerOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(staleConsumerOffset, freshConsumerOffset)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val schema = StructType(Seq(StructField("v", LongType)))
    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, schema, Map.empty,
      offsetProvider = mockOffsetProvider)

    val sparkCachedStart = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 80L))
    val batchEnd = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 60L))

    withConf(Streaming.Transactional, true) {
      source.latestOffset(null, ReadLimit.maxRows(10))
      val rdd = source.getBatch(Some(sparkCachedStart), batchEnd)
        .queryExecution.logical.asInstanceOf[LogicalRDD].rdd
      rdd.name shouldBe "yt"
      val ranges = rdd.partitions.map(_.asInstanceOf[YtQueueRange])
      ranges should have length 1
      val range = ranges.head
      range.tabletIndex shouldBe 0
      range.lowerIndex shouldBe (freshConsumerOffset.partitions(0) + 1L)
      range.upperIndex shouldBe (batchEnd.partitions(0) + 1L)
    }
  }

  it should "fall back to empty RDD in getBatch when consumer offset is past batch end" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val staleConsumerOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 30L))
    val freshConsumerOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 70L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(staleConsumerOffset, freshConsumerOffset)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val schema = StructType(Seq(StructField("v", LongType)))
    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, schema, Map.empty,
      offsetProvider = mockOffsetProvider)

    val sparkCachedStart = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 40L))
    val batchEnd = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 60L))

    source.latestOffset(null, ReadLimit.maxRows(10))
    val rdd = source.getBatch(Some(sparkCachedStart), batchEnd)
      .queryExecution.logical.asInstanceOf[LogicalRDD].rdd
    rdd.name shouldBe "empty"
  }

  it should "(transactional mode) use empty consumer offset in latestOffset (missing partitions read from -1 via maxOffset keys)" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val freshEmpty = YtQueueOffset(cluster, queuePath, SortedMap.empty[Int, Long])
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(freshEmpty)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, new StructType(),
      Map("max_rows_per_partition" -> "10"), offsetProvider = mockOffsetProvider)

    val sparkCached = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 29L))

    withConf(Streaming.Transactional, true) {
      val resultOffset = source.latestOffset(sparkCached, ReadLimit.maxRows(10)).asInstanceOf[YtQueueOffset]
      resultOffset.partitions shouldBe SortedMap(0 -> 9L)
    }
  }

  it should "(non-transactional mode, BUG REPRO) advance from Spark startOffset on second batch when consumer is still empty" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val emptyConsumer = YtQueueOffset(cluster, queuePath, SortedMap(0 -> -1L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 1000000L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(emptyConsumer)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, new StructType(),
      Map("max_rows_per_partition" -> "50000"), offsetProvider = mockOffsetProvider)

    val endOfBatch0 = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 49999L))

    val resultOffset = source.latestOffset(endOfBatch0, ReadLimit.maxRows(50000)).asInstanceOf[YtQueueOffset]
    resultOffset.partitions shouldBe SortedMap(0 -> 99999L)
  }

  it should "(non-transactional mode) use Spark startOffset in latestOffset when checkpoint is ahead of consumer" in {
    val source = sourceWith(
      offsetProviderWith(singlePartitionOffset(50L), singlePartitionOffset(1000L)),
      parameters = Map("max_rows_per_partition" -> "10")
    )

    val sparkAhead = singlePartitionOffset(80L)

    val resultOffset = source.latestOffset(sparkAhead, ReadLimit.maxRows(10)).asInstanceOf[YtQueueOffset]
    resultOffset.partitions shouldBe SortedMap(0 -> 90L)
  }

  it should "(non-transactional mode) fall back to consumer when Spark startOffset is behind consumer" in {
    val source = sourceWith(
      offsetProviderWith(singlePartitionOffset(50L), singlePartitionOffset(1000L)),
      parameters = Map("max_rows_per_partition" -> "10")
    )

    val sparkBehind = singlePartitionOffset(20L)

    val resultOffset = source.latestOffset(sparkBehind, ReadLimit.maxRows(10)).asInstanceOf[YtQueueOffset]
    resultOffset.partitions shouldBe SortedMap(0 -> 60L)
  }

  it should "(non-transactional mode) getBatch uses Spark start when ahead of consumer" in {
    val schema = StructType(Seq(StructField("v", LongType)))
    val source = sourceWith(offsetProviderWith(singlePartitionOffset(50L), singlePartitionOffset(1000L)),
      schema = schema)

    val sparkStart = singlePartitionOffset(80L)
    val batchEnd = singlePartitionOffset(100L)

    val rdd = source.getBatch(Some(sparkStart), batchEnd)
      .queryExecution.logical.asInstanceOf[LogicalRDD].rdd
    rdd.name shouldBe "yt"
    val ranges = rdd.partitions.map(_.asInstanceOf[YtQueueRange])
    ranges should have length 1
    val range = ranges.head
    range.lowerIndex shouldBe 81L
    range.upperIndex shouldBe 101L
  }

  it should "call provider's advance on commit with last fetched offset" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]
    val lastCommitted = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    val newCommit = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 60L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(lastCommitted)

    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)

    source.commit(newCommit)

    verify(mockOffsetProvider, times(1)).advance(
      eqTo(consumerPath),
      eqTo(newCommit),
      eqTo(lastCommitted),
      eqTo(None),
      eqTo(None)
    )(any[CompoundClient])
  }

  it should "skip commit when transactional is enabled and no transaction context is set" in {
    val mockOffsetProvider = mock[YtQueueOffsetProvider]
    val zero = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 0L))
    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(zero)

    val source = new YtStreamingSource(spark.sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)

    val newCommit = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 10L))

    withConf(Streaming.Transactional, true) {
      source.commit(newCommit)
    }

    verify(mockOffsetProvider, never).advance(
      any[String], any[YtQueueOffset], any[YtQueueOffset], any[Option[YtQueueOffset]], any[Option[String]]
    )(any[CompoundClient])
  }

  it should "throw on getOffset (only latestOffset is supported)" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]
    val zero = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 0L))
    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(zero)

    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)

    a[UnsupportedOperationException] should be thrownBy source.getOffset
  }

  it should "return composite limit when max_rows_per_partition is set, allAvailable otherwise" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]
    val zero = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 0L))
    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(zero)

    val withLimit = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(),
      Map("max_rows_per_partition" -> "7"), offsetProvider = mockOffsetProvider)
    withLimit.getDefaultReadLimit shouldBe a[org.apache.spark.sql.connector.read.streaming.CompositeReadLimit]

    val withoutLimit = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)
    withoutLimit.getDefaultReadLimit shouldBe ReadLimit.allAvailable()
  }

  it should "return full maxOffset from latestOffset when no max_rows_per_partition is set" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val current = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 5L, 1 -> 7L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L, 1 -> 200L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(current)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)

    source.latestOffset(null, ReadLimit.allAvailable()).asInstanceOf[YtQueueOffset] shouldBe maxOffset
  }

  it should "clamp each partition independently in latestOffset for multi-partition queues" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val current = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L, 1 -> 95L, 2 -> 0L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L, 1 -> 100L, 2 -> 5L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(current)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient])).thenReturn(Success(maxOffset))

    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(),
      Map("max_rows_per_partition" -> "10"), offsetProvider = mockOffsetProvider)

    val result = source.latestOffset(null, ReadLimit.maxRows(10)).asInstanceOf[YtQueueOffset]
    result.partitions shouldBe SortedMap(0 -> 60L, 1 -> 100L, 2 -> 5L)
  }

  it should "return null from latestOffset when getMaxOffset fails on first call" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val current = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 0L))
    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(current)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Failure(new RuntimeException("YT temporarily unavailable")))

    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)

    source.latestOffset(null, ReadLimit.allAvailable()) shouldBe null
  }

  it should "return None from getMaxOffset when newMaxOffset is below lastCommittedOffset" in {
    val source = sourceWith(
      offsetProviderWith(singlePartitionOffset(80L), singlePartitionOffset(50L)),
      sqlContext = mock[SQLContext]
    )

    source.getMaxOffset shouldBe None
  }

  it should "keep cached maxOffset when newMaxOffset is below previously cached value" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProvider]

    val current = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 0L))
    val firstMax = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))
    val regressedMax = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 60L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(current)
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Success(firstMax), Success(regressedMax))

    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(), Map.empty,
      offsetProvider = mockOffsetProvider)

    source.getMaxOffset shouldBe Some(firstMax)
    source.getMaxOffset shouldBe Some(firstMax)
  }

  it should "build one YtQueueRange per partition in getBatch for multi-partition queues" in {
    val current = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 5L, 1 -> 9L, 2 -> -1L))
    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L, 1 -> 50L, 2 -> 50L))

    val schema = StructType(Seq(StructField("v", LongType)))
    val source = sourceWith(offsetProviderWith(current, maxOffset), schema = schema)

    val batchEnd = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 9L, 1 -> 14L, 2 -> 4L))

    val rdd = source.getBatch(None, batchEnd)
      .queryExecution.logical.asInstanceOf[LogicalRDD].rdd
    rdd.name shouldBe "yt"
    val ranges = rdd.partitions.map(_.asInstanceOf[YtQueueRange]).toSeq
    ranges should contain theSameElementsAs Seq(
      YtQueueRange(0, 6L, 10L),
      YtQueueRange(1, 10L, 15L),
      YtQueueRange(2, 0L, 5L)
    )
  }

  it should "return empty RDD from getBatch when start equals end (no-data microbatch)" in {
    val schema = StructType(Seq(StructField("v", LongType)))
    val source = sourceWith(offsetProviderWith(singlePartitionOffset(0L), singlePartitionOffset(100L)),
      schema = schema)

    val sameOffset = singlePartitionOffset(30L)
    val rdd = source.getBatch(Some(sameOffset), sameOffset)
      .queryExecution.logical.asInstanceOf[LogicalRDD].rdd
    rdd.name shouldBe "empty"
  }

  it should "deserialize SerializedOffset end in getBatch and build correct ranges" in {
    val schema = StructType(Seq(StructField("v", LongType)))
    val source = sourceWith(offsetProviderWith(singlePartitionOffset(4L), singlePartitionOffset(100L)),
      schema = schema)

    val end = singlePartitionOffset(9L)
    val serializedEnd = SerializedOffset(end.json())

    val rdd = source.getBatch(None, serializedEnd)
      .queryExecution.logical.asInstanceOf[LogicalRDD].rdd
    rdd.name shouldBe "yt"
    val ranges = rdd.partitions.map(_.asInstanceOf[YtQueueRange]).toSeq
    ranges should contain theSameElementsAs Seq(YtQueueRange(0, 5L, 10L))
  }
}
