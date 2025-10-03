package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchersSugar.{any, eqTo}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.test.LocalYtClient
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.collection.SortedMap
import scala.util.{Failure, Success}

class YtStreamingSourceTest extends AnyFlatSpec with Matchers with MockitoSugar with LocalYtClient {
  private val consumerPath = "//tmp/path/to/consumer"
  private val queuePath = "//tmp/path/to/queue"
  private val cluster = YtWrapper.clusterName()

  private def createFixture(parameters: Map[String, String] = Map.empty): (YtStreamingSource, YtQueueOffsetProviderTrait, SQLContext) = {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProviderTrait]
    val source = new YtStreamingSource(sqlContext, consumerPath, queuePath, new StructType(), parameters,
      offsetProvider = mockOffsetProvider)
    (source, mockOffsetProvider, sqlContext)
  }

  behavior of "YtStreamingSource"

  it should "use provider for current offset in latestOffset if start is not defined" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProviderTrait]

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

  it should "fail if start offset is less than committed offset" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProviderTrait]

    val lastCommittedOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient]))
      .thenReturn(lastCommittedOffset)

    val maxOffset = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 100L))
    when(mockOffsetProvider.getMaxOffset(any[String], any[String])(any[CompoundClient]))
      .thenReturn(Success(maxOffset))

    val source = new YtStreamingSource(
      sqlContext,
      consumerPath,
      queuePath,
      new StructType(),
      parameters = Map("max_rows_per_partition" -> "10"),
      offsetProvider = mockOffsetProvider
    )

    source.getLastCommittedOffset shouldBe lastCommittedOffset

    val startOffsetFromCheckpoint = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 40L))
    val latestOffset = source.latestOffset(startOffsetFromCheckpoint, ReadLimit.maxRows(10))

    latestOffset.asInstanceOf[YtQueueOffset].partitions shouldBe SortedMap(0 -> 60L)
  }

  it should "return cached offset on provider failure" in {
    val sqlContext = mock[SQLContext]
    val mockOffsetProvider = mock[YtQueueOffsetProviderTrait]
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

    source.getMaxOffset shouldBe Some(initialMaxOffset)
    source.getMaxOffset shouldBe Some(initialMaxOffset)
    verify(mockOffsetProvider, times(2)).getMaxOffset(eqTo(cluster), eqTo(queuePath))(any[CompoundClient])
  }

  it should "call provider's advance on commit with last fetched offset" in {
    val (source, mockOffsetProvider, _) = createFixture()
    val lastCommitted = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 50L))
    val newCommit = YtQueueOffset(cluster, queuePath, SortedMap(0 -> 60L))

    when(mockOffsetProvider.getCurrentOffset(any[String], any[String], any[String])(any[CompoundClient])).thenReturn(lastCommitted)
    source.getLastCommittedOffset

    source.commit(newCommit)

    verify(mockOffsetProvider, times(1)).advance(
      eqTo(consumerPath),
      eqTo(newCommit),
      eqTo(lastCommitted),
      eqTo(None)
    )(any[CompoundClient])
  }
}