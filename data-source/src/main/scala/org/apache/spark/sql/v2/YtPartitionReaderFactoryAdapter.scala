package org.apache.spark.sql.v2

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils.bytesReadReporter
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch, SingleValueColumnVector, YtVectorizedReader}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.common.utils.SegmentSet
import tech.ytsaurus.spyt.format.YtInputSplit
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate.YtPartitionedFile
import tech.ytsaurus.spyt.format.conf.FilterPushdownConfig
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.{CountOptimizationEnabled, VectorizedCapacity}
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.logger.{TaskInfo, YtDynTableLoggerConfig}
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import java.util.UUID

case class YtPartitionReaderFactoryAdapter(sqlConf: SQLConf,
                                           broadcastedConf: Broadcast[SerializableConfiguration],
                                           dataSchema: StructType,
                                           readDataSchema: StructType,
                                           partitionSchema: StructType,
                                           options: Map[String, String],
                                           pushedFilterSegments: SegmentSet,
                                           filterPushdownConf: FilterPushdownConfig,
                                           ytLoggerConfig: Option[YtDynTableLoggerConfig])
  extends PartitionReaderFactoryAdapter with Logging {

  private val idPrefix: String = s"YtPartitionReaderFactory-${UUID.randomUUID()}"

  private val resultSchema = StructType(readDataSchema.fields)
  private val ytClientConf = ytClientConfiguration(sqlConf)
  private val arrowEnabled: Boolean = YtReaderOptions.arrowEnabled(options, sqlConf)
  private val optimizedForScan: Boolean = YtReaderOptions.optimizedForScan(options)
  private val readBatch: Boolean = YtReaderOptions.canReadBatch(readDataSchema, optimizedForScan, arrowEnabled)
  private val returnBatch: Boolean = readBatch && YtReaderOptions.supportBatch(resultSchema, sqlConf)
  private val batchMaxSize = sqlConf.ytConf(VectorizedCapacity)
  private val countOptimizationEnabled = sqlConf.ytConf(CountOptimizationEnabled)

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    returnBatch
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    buildLockedSplitReader(file) { case (split, path) =>
      implicit val yt: CompoundClient = YtClientProvider.ytClientWithProxy(ytClientConf, path.ypath.cluster, idPrefix)
      val reader = if (readBatch) {
        createVectorizedReader(split, returnBatch = false, path)
      } else {
        createRowBaseReader(split, path)
      }

      val fileReader = new PartitionReader[InternalRow] {
        override def next(): Boolean = reader.nextKeyValue()

        override def get(): InternalRow = reader.getCurrentValue.asInstanceOf[InternalRow]

        override def close(): Unit = {
          reader.close()
        }
      }

      new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
        partitionSchema, file.partitionValues)
    }
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    buildLockedSplitReader(file) { case (split, path) =>
      implicit val yt: CompoundClient = YtClientProvider.ytClientWithProxy(ytClientConf, path.ypath.cluster, idPrefix)
      val vectorizedReader = createVectorizedReader(split, returnBatch = true, path)
      new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = vectorizedReader.nextKeyValue()

        override def get(): ColumnarBatch = {
          val sourceBatch = vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]
          val capacity = sourceBatch.numRows()
          val schemaCols = sourceBatch.numCols()
          if (partitionSchema != null && partitionSchema.nonEmpty) {
            val columnVectors = new Array[ColumnVector](sourceBatch.numCols() + partitionSchema.fields.length)
            for (i <- 0 until schemaCols) {
              columnVectors(i) = sourceBatch.column(i)
            }
            partitionSchema.fields.zipWithIndex.foreach { case (field, index) =>
              columnVectors(index + schemaCols) = new SingleValueColumnVector(capacity, field.dataType,
                file.partitionValues, index)
            }
            new ColumnarBatch(columnVectors, capacity)
          }
          else
            sourceBatch
        }

        override def close(): Unit = {
          vectorizedReader.close()
        }
      }
    }
  }

  private def buildLockedSplitReader[T](file: PartitionedFile)
                                       (splitReader: (YtInputSplit, YtHadoopPath) => PartitionReader[T]): PartitionReader[T] = {
    file match {
      case ypf: YtPartitionedFile =>
        val split = createSplit(ypf)
        splitReader(split, ypf.delegate.hadoopPath)
      case _ =>
        throw new IllegalArgumentException(s"Partitions of type ${file.getClass.getSimpleName} are not supported")
    }
  }

  private def createSplit(file: YtPartitionedFile): YtInputSplit = {
    val log = LoggerFactory.getLogger(getClass)
    val ytLoggerConfigWithTaskInfo = ytLoggerConfig.map(_.copy(taskContext = Some(TaskInfo(TaskContext.get()))))
    val split = YtInputSplit(file, resultSchema, pushedFilterSegments, filterPushdownConf, ytLoggerConfigWithTaskInfo)

    log.info(s"Reading ${split.ytPath}, " +
      s"read batch: $readBatch, return batch: $returnBatch, arrowEnabled: $arrowEnabled, " +
      s"pushdown config: $filterPushdownConf, detailed yPath: ${split.ytPathWithFiltersDetailed}")

    split
  }

  private def createRowBaseReader(split: YtInputSplit, hadoopPath: YtHadoopPath)
                                 (implicit yt: CompoundClient): RecordReader[Void, InternalRow] = {
    val iter = YtWrapper.readTable(
      split.ytPathWithFiltersDetailed,
      InternalRowDeserializer.getOrCreate(resultSchema),
      ytClientConf.timeout, hadoopPath.ypath.transaction,
      bytesReadReporter(broadcastedConf)
    )
    val unsafeProjection = UnsafeProjection.create(resultSchema)

    new RecordReader[Void, InternalRow] {
      private var current: InternalRow = _

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

      override def nextKeyValue(): Boolean = {
        if (iter.hasNext) {
          current = unsafeProjection.apply(iter.next())
          true
        } else false
      }

      override def getCurrentKey: Void = {
        null
      }

      override def getCurrentValue: InternalRow = {
        current
      }

      override def getProgress: Float = 0.0f

      override def close(): Unit = {
        iter.close()
      }
    }
  }

  private def createVectorizedReader(split: YtInputSplit, returnBatch: Boolean, hadoopPath: YtHadoopPath)
                                    (implicit yt: CompoundClient): YtVectorizedReader = {
    new YtVectorizedReader(
      split = split,
      batchMaxSize = batchMaxSize,
      returnBatch = returnBatch,
      arrowEnabled = arrowEnabled,
      optimizedForScan = optimizedForScan,
      timeout = ytClientConf.timeout,
      reportBytesRead = bytesReadReporter(broadcastedConf),
      countOptimizationEnabled = countOptimizationEnabled,
      hadoopPath = hadoopPath,
    )
  }
}
