package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.internal.Logging
import org.apache.spark.{InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleReader}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.CompletionIterator
import tech.ytsaurus.client.rows.UnversionedRow
import tech.ytsaurus.client.{AsyncReader, CompoundClient}
import tech.ytsaurus.client.request.CreateShuffleReader
import tech.ytsaurus.ysontree.YTreeNode

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters._

class YTsaurusShuffleReader[K, C](compoundHandle: CompoundShuffleHandle[K, _, C],
                                  startMapIndex: Int,
                                  endMapIndex: Int,
                                  startPartition: Int,
                                  endPartition: Int,
                                  context: TaskContext,
                                  readMetrics: ShuffleReadMetricsReporter,
                                  ytClient: CompoundClient,
                                  readConfigOpt: Option[YTreeNode]
                                 ) extends ShuffleReader[K, C] with Logging {
  private val serializer = compoundHandle.baseHandle.dependency.serializer.newInstance()
  private val blockId = ShuffleBlockId(compoundHandle.shuffleId, startMapIndex, startPartition)

  private val deserializer: InputStream => Iterator[(K, C)] = { is =>
    val wrappedStream = SparkEnv.get.serializerManager.wrapStream(blockId, is)
    serializer.deserializeStream(wrappedStream).asKeyValueIterator.asInstanceOf[Iterator[(K, C)]]
  }

  private val readerSupplier: Int => CompletableFuture[AsyncReader[UnversionedRow]] = partitionIndex => {
    logTrace(s"SHUFFLE ${compoundHandle.shuffleId} READING PARTITION: ${partitionIndex}")
    val reqBuilder = CreateShuffleReader.builder()
      .setHandle(compoundHandle.ytHandle)
      .setPartitionIndex(partitionIndex)
      .setRange(new CreateShuffleReader.Range(startMapIndex, endMapIndex))

    readConfigOpt.foreach(reqBuilder.setConfig)

    ytClient.createShuffleReader(reqBuilder.build())
  }

  override def read(): Iterator[Product2[K, C]] = {
    val shuffleRowInputStream = new ShuffleRowInputStream(startPartition, endPartition, readerSupplier, readMetrics)
    val recordIter = deserializer(shuffleRowInputStream)

    // Here and below until the end of the method - a copypaste from
    // org.apache.spark.shuffle.BlockStoreShuffleReader.read method
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
  }
}

class ShuffleRowInputStream(
  startPartition: Int,
  endPartition: Int,
  readerSupplier: Int => CompletableFuture[AsyncReader[UnversionedRow]],
  readMetrics: ShuffleReadMetricsReporter
) extends InputStream {

  private val partitionIterator: Iterator[Int] = (startPartition until endPartition).iterator
  private var rowIterator: Iterator[UnversionedRow] = Iterator.empty
  private var reader: AsyncReader[UnversionedRow] = _
  private var rowBytesReader: ByteArrayInputStream = _

  private def hasNext: Boolean = {
    while (!rowIterator.hasNext && (reader != null || partitionIterator.hasNext)) {
      if (reader == null) {
        reader = readerSupplier(partitionIterator.next()).join()
      }

      val batch = reader.next().join()

      if (batch != null) {
        rowIterator = batch.iterator().asScala
      } else {
        reader = null
      }
    }
    rowIterator.hasNext
  }

  private def next(): UnversionedRow = rowIterator.next()

  override def read(): Int = {
    throw new UnsupportedOperationException("ShuffleRowInputStream doesn't support reading per byte, please use " +
      "method int read(byte b[], int off, int len)")
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (len == 0) {
      return 0
    }

    var bytesRead = 0
    var pos = off
    while (bytesRead < len && (rowBytesReader != null || hasNext)) {
      if (rowBytesReader == null && hasNext) {
        rowBytesReader = new ByteArrayInputStream(next().getValues.get(1).bytesValue())
      }
      if (rowBytesReader != null) {
        val bytesReadFromRow = rowBytesReader.read(b, pos, len - bytesRead)
        bytesRead += bytesReadFromRow
        pos += bytesReadFromRow
        if (rowBytesReader.available() == 0) {
          rowBytesReader = null
        }
      }
    }

    if (bytesRead > 0) {
      readMetrics.incRemoteBytesRead(bytesRead)
      bytesRead
    } else -1
  }
}
