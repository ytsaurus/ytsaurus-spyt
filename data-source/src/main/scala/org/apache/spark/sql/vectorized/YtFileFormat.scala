package org.apache.spark.sql.vectorized

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v2.YtUtils.bytesReadReporter
import org.apache.spark.sql.v2.{YtReaderOptions, YtUtils}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.common.utils.YtReadingUtils
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate.YtPartitionedFile
import tech.ytsaurus.spyt.format._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.YtReadSettingsFactory
import tech.ytsaurus.spyt.format.conf.{FilterPushdownConfig, SparkYtWriteConfiguration}
import tech.ytsaurus.spyt.logger.YtDynTableLoggerConfig
import tech.ytsaurus.spyt.streaming.YtStreamingProvider
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.table.YtReadContext

import java.util.ServiceLoader
import scala.collection.JavaConverters._


class YtFileFormat extends FileFormat with DataSourceRegister with StreamSourceProvider with StreamSinkProvider with Serializable {
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {
    YtUtils.inferSchema(sparkSession, options, files)
  }


  override def vectorTypes(requiredSchema: StructType, partitionSchema: StructType,
    sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.length)(classOf[ColumnVector].getName))
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType,
    partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String],
    hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    import tech.ytsaurus.spyt.wrapper.config._
    val ytClientConf = ytClientConfiguration(hadoopConf)

    val sqlConf = sparkSession.sqlContext.conf
    val arrowEnabledValue = YtReaderOptions.arrowEnabled(options, sqlConf)
    val optimizedForScanValue = YtReaderOptions.optimizedForScan(options)
    val fullReadAllowedValue = YtReaderOptions.fullReadAllowed(options)
    val readBatch = YtReaderOptions.canReadBatch(requiredSchema, options, sqlConf)
    val returnBatch = readBatch && YtReaderOptions.supportBatch(requiredSchema, sqlConf)
    val filterPushdownConfig = FilterPushdownConfig(sparkSession)

    val batchMaxSize = hadoopConf.ytConf(VectorizedCapacity)
    val countOptimizationEnabled = hadoopConf.ytConf(CountOptimizationEnabled)
    val readSettings = YtReadSettingsFactory.fromSpark(sparkSession)

    val log = LoggerFactory.getLogger(getClass)
    log.info(s"Batch read enabled: $readBatch")
    log.info(s"Full read allowed: $fullReadAllowedValue")
    log.info(s"Batch return enabled: $returnBatch")
    log.info(s"Optimized for scan: $optimizedForScanValue")
    log.info(s"Arrow enabled: $arrowEnabledValue")
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val ytLoggerConfig = YtDynTableLoggerConfig.fromSpark(sparkSession)

    {
      case ypf: YtPartitionedFile @unchecked =>
        val log = LoggerFactory.getLogger(getClass)
        val yt: CompoundClient =
          YtClientProvider.ytClientWithProxy(ytClientConf, ypf.delegate.cluster, Some(StatisticsReporter))
        YtReadContext.withContext(yt, readSettings) { implicit ctx =>
          StatisticsReporter.registerReadMetrics(ctx.requestId, bytesReadReporter(broadcastedConf))
          val split = YtInputSplit(ypf, requiredSchema, filterPushdownConfig = filterPushdownConfig,
            ytLoggerConfig = ytLoggerConfig)

          log.info(s"Reading ${split.ytPathWithFilters}")
          if (readBatch) {
            val ytVectorizedReader = new YtVectorizedReader(
              split = split,
              batchMaxSize = batchMaxSize,
              returnBatch = returnBatch,
              arrowEnabled = arrowEnabledValue,
              optimizedForScan = optimizedForScanValue,
              fullReadAllowed = fullReadAllowedValue,
              timeout = ytClientConf.timeout,
              countOptimizationEnabled = countOptimizationEnabled,
              hadoopPath = ypf.delegate.hadoopPath
            )
            val iter = new RecordReaderIterator(ytVectorizedReader)
            Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
            if (!returnBatch) {
              val unsafeProjection = if (arrowEnabledValue) {
                ColumnarBatchRowUtils.unsafeProjection(requiredSchema)
              } else {
                UnsafeProjection.create(requiredSchema)
              }
              iter.asInstanceOf[Iterator[InternalRow]].map(unsafeProjection)
            } else {
              iter.asInstanceOf[Iterator[InternalRow]]
            }
          } else {
            val tableIterator = YtReadingUtils.createRowBaseReader(split, None, requiredSchema, ytClientConf)
            val unsafeProjection = UnsafeProjection.create(requiredSchema)
            tableIterator.map(unsafeProjection(_))
          }
        }
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    YtOutputWriterFactory.create(
      SparkYtWriteConfiguration(sparkSession.sqlContext),
      ytClientConfiguration(sparkSession),
      options,
      dataSchema,
      job.getConfiguration
    )
  }

  override def shortName(): String = "yt"

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = true

  private lazy val streamingProvider: YtStreamingProvider = {
    ServiceLoader.load(classOf[YtStreamingProvider]).iterator().asScala.toSeq.headOption.getOrElse(
      throw new UnsupportedOperationException(
        s"Streaming is not supported. Should implement ${classOf[YtStreamingProvider].getName}")
    )
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    YtReaderOptions.supportBatch(dataSchema, sparkSession.sqlContext.conf)
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String,
    parameters: Map[String, String]): (String, StructType) = {
    streamingProvider.sourceSchema(sqlContext, schema, providerName, parameters)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): Source = {
    streamingProvider.createSource(sqlContext, metadataPath, schema, providerName, parameters)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String],
    outputMode: OutputMode): Sink = {
    streamingProvider.createSink(sqlContext, parameters, partitionColumns, outputMode)
  }
}
