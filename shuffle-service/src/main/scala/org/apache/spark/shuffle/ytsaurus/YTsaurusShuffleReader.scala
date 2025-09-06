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

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.nio.ByteBuffer
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
    val recordIter =
      new ShuffleRecordIterator[K, C](startPartition, endPartition, readerSupplier, deserializer, readMetrics)

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

class ShuffleRecordIterator[K, C](
  startPartition: Int,
  endPartition: Int,
  readerSupplier: Int => CompletableFuture[AsyncReader[UnversionedRow]],
  deserializer: InputStream => Iterator[(K, C)],
  readMetrics: ShuffleReadMetricsReporter
) extends Iterator[(K, C)] {

  private val partitionIterator: Iterator[Int] = (startPartition until endPartition).iterator
  private var reader: AsyncReader[UnversionedRow] = _
  private var rowIterator: Iterator[UnversionedRow] = Iterator.empty
  private var currentRow: UnversionedRow = _
  private var recordIterator: Iterator[(K, C)] = Iterator.empty

  def hasNext: Boolean = {
    if (!recordIterator.hasNext) {
      recordIterator = nextInputStream() match {
        case Some(inputStream) => deserializer(inputStream)
        case None => Iterator.empty
      }
    }
    recordIterator.hasNext
  }

  private def shuffleRowBytes(shuffleRow: UnversionedRow): Array[Byte] = {
    shuffleRow.getValues.get(1).bytesValue()
  }

  private def mapIdByRow(shuffleRow: UnversionedRow): Long = {
    ByteBuffer.wrap(shuffleRowBytes(shuffleRow), 0, 8).getLong
  }

  private def nextInputStream(): Option[InputStream] = {
    val streamEnumeration = new java.util.Enumeration[InputStream] {
      private var nextStream: InputStream = _
      private var mapId: Option[Long] = None

      override def hasMoreElements: Boolean = {
        if (nextStream != null) {
          return true
        }
        val nextRowOpt = nextRow()
        if (nextRowOpt.isEmpty) {
          return false
        }
        currentRow = nextRowOpt.get
        val rowMapId = mapIdByRow(currentRow)
        if (mapId.nonEmpty && mapId.get != rowMapId) {
          return false
        }
        if (mapId.isEmpty) {
          mapId = Some(rowMapId)
        }
        val batchBytes = shuffleRowBytes(currentRow)
        readMetrics.incRemoteBytesRead(batchBytes.length)
        nextStream = new ByteArrayInputStream(batchBytes, 8, batchBytes.length - 8)
        currentRow = null
        true
      }

      override def nextElement(): InputStream = {
        val next = nextStream
        nextStream = null
        next
      }
    }

    if (streamEnumeration.hasMoreElements) {
      Some(new SequenceInputStream(streamEnumeration))
    } else {
      None
    }
  }

  private def nextRow(): Option[UnversionedRow] = {
    if (currentRow != null) {
      return Some(currentRow)
    }
    while (!rowIterator.hasNext && (reader != null || partitionIterator.hasNext)) {
      if (reader != null) {
        val batch = reader.next().join()

        if (batch != null) {
          rowIterator = batch.iterator().asScala
        } else {
          reader = null
        }
      } else {
        reader = readerSupplier(partitionIterator.next()).join()
      }
    }

    if (rowIterator.hasNext) {
      Some(rowIterator.next())
    } else {
      None
    }
  }

  def next(): (K, C) = recordIterator.next()
}
