package org.apache.spark.sql.vectorized

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.spyt.format.batch.{ArrowBatchReader, BatchReader, EmptyColumnsBatchReader, WireRowBatchReader}
import tech.ytsaurus.spyt.format.{YPathUtils, YtInputSplit}
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.serializers.ArrayAnyDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.{SyncTableIterator, TableCopyByteStreamBase, TableIterator}

import scala.concurrent.duration.Duration

class YtVectorizedReader(split: YtInputSplit,
                         batchMaxSize: Int,
                         returnBatch: Boolean,
                         arrowEnabled: Boolean,
                         optimizedForScan: Boolean,
                         timeout: Duration,
                         reportBytesRead: Long => Unit,
                         countOptimizationEnabled: Boolean,
                         hadoopPath: YtHadoopPath,
                         distributedReadingEnabled: Boolean)
                        (implicit yt: CompoundClient) extends RecordReader[Void, Object] {
  private val log = LoggerFactory.getLogger(getClass)
  private var _batchIdx = 0

  private val batchReader: BatchReader = {
    val path = split.ytPathWithFilters
    log.info(s"Reading from $path")
    val schema = split.schema
    val totalRowCount = YPathUtils.rowCount(path)
    if (countOptimizationEnabled && schema.isEmpty && totalRowCount.isDefined) {
      new EmptyColumnsBatchReader(totalRowCount.get) // Empty schemas always batch readable
    } else if (arrowEnabled && optimizedForScan) {
      createArrowBatchReader(path, schema)
    } else {
      createWireRowBatchReader(path, schema)
    }
  }

  private def createArrowBatchReader(path: YPath, schema: StructType): ArrowBatchReader = {
    val ytSchema = TableSchema.fromYTree(YtWrapper.attribute(path, "schema", hadoopPath.ypath.transaction))
    val stream = if (distributedReadingEnabled) {
      YtWrapper.createTablePartitionArrowStream(split.file.delegate.cookie.get, reportBytesRead)
    } else {
      YtWrapper.readTableArrowStream(path, timeout, hadoopPath.ypath.transaction, reportBytesRead)
    }
    new ArrowBatchReader(stream, schema, ytSchema)
  }

  private def createWireRowBatchReader(path: YPath, schema: StructType) = {
    val deserializer = ArrayAnyDeserializer.getOrCreate(schema)

    val rowIterator: TableIterator[Array[Any]] = if (distributedReadingEnabled) {
      YtWrapper.createTablePartitionReader(split.file.delegate.cookie.get, deserializer)
    } else {
      YtWrapper.readTable(path, deserializer, timeout, hadoopPath.ypath.transaction, reportBytesRead)
    }
    new WireRowBatchReader(rowIterator, batchMaxSize, schema)
  }

  private lazy val unsafeProjection = if (arrowEnabled) {
    ColumnarBatchRowUtils.unsafeProjection(split.schema)
  } else {
    UnsafeProjection.create(split.schema)
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

  override def nextKeyValue(): Boolean = {
    if (returnBatch) {
      nextBatch
    } else {
      _batchIdx += 1
      if (_batchIdx >= batchReader.currentBatchSize) {
        if (nextBatch) {
          _batchIdx = 0
          true
        } else {
          false
        }
      } else {
        true
      }
    }
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: AnyRef = {
    if (returnBatch) {
      batchReader.currentBatch
    } else {
      unsafeProjection.apply(batchReader.currentBatch.getRow(_batchIdx))
    }
  }

  override def getProgress: Float = 1f

  override def close(): Unit = {
    batchReader.close()
  }

  private def nextBatch: Boolean = {
    val next = batchReader.nextBatch
    _batchIdx = 0
    next
  }
}
