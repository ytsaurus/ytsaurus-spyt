package tech.ytsaurus.spyt.format

import org.apache.spark.TaskContext
import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings._
import tech.ytsaurus.spyt.wrapper.config._
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.{InternalRowSerializer, WriteSchemaConverter}
import tech.ytsaurus.spyt.wrapper.LogLazy
import tech.ytsaurus.client.request.{SerializationContext, TransactionalOptions, WriteSerializationContext, WriteTable}
import tech.ytsaurus.client.{AsyncWriter, CompoundClient}
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}

abstract class AbstractYtOutputWriter[W <: AsyncWriter[InternalRow]](
  schema: StructType,
  writeConfiguration: SparkYtWriteConfiguration,
  options: Map[String, String]) extends OutputWriter with LogLazy {

  import writeConfiguration._

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  protected val requestId: GUID = GUID.create()
  WriteStatisticsReporter.registerRequest(requestId, TaskContext.get())

  private val writer: W = initializeWriter()
  private var writeFuture: CompletableFuture[Void] = CompletableFuture.completedFuture(null);

  private var buffer = createBuffer()

  initialize()

  override def write(record: InternalRow): Unit = {
    try {
      YtMetricsRegister.time(writeTime, writeTimeSum) {
        buffer.add(record.copy())
        if (buffer.size() == bufferSize) {
          writeMiniBatch()
          buffer = createBuffer()
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("Write failed, cancelling writer")
        writer.cancel()
        WriteStatisticsReporter.unregisterRequest(requestId)
        log.warn("Write failed, writer is cancelled")
        throw e
    }
  }

  private def writeMiniBatch(): Unit = {
    log.debugLazy(s"Writing mini batch of size $bufferSize")
    YtMetricsRegister.time(writeBatchTime, writeBatchTimeSum) {
      writeFuture.get(timeout.toMillis, TimeUnit.MILLISECONDS)
      writeFuture = writer.write(buffer)
    }
    log.debugLazy(s"Mini batch written")
  }

  override def close(): Unit = {
    log.debugLazy("Closing YtOutputWriter")
    if (buffer.size() != 0) {
      log.debugLazy(s"Writing last batch, list size: ${buffer.size()}, writer: $this ")
      writeMiniBatch()
    }
    writeFuture.get(timeout.toMillis, TimeUnit.MILLISECONDS)
    YtMetricsRegister.time(writeCloseTime, writeCloseTimeSum) {
      finishWriter(writer, timeout.toMillis)
    }
    WriteStatisticsReporter.unregisterRequest(requestId)
  }

  protected def initializeWriter(): W

  protected def finishWriter(writer: W, timeoutMs: Long): Unit

  private def createBuffer() = new util.ArrayList[InternalRow](bufferSize)

  protected def createSerializationContext(): SerializationContext[InternalRow] = {
    new WriteSerializationContext(new InternalRowSerializer(schema, WriteSchemaConverter(options)))
  }

  protected def initialize(): Unit = {
    YtMetricsRegister.register()
  }
}

class YtOutputWriter(richPath: YPathEnriched,
                     schema: StructType,
                     writeConfiguration: SparkYtWriteConfiguration,
                     options: Map[String, String])(implicit client: CompoundClient)
  extends AbstractYtOutputWriter[AsyncWriter[InternalRow]](schema, writeConfiguration, options) {

  val path: String = richPath.toStringPath

  private def transactionGuid: String = richPath.transaction.get

  protected override def initializeWriter(): AsyncWriter[InternalRow] = {
    val appendPath = richPath.withAttr("append", "true").toYPath
    log.debugLazy(s"Initialize new write: $appendPath, transaction: $transactionGuid")
    val request = WriteTable.builder[InternalRow]()
      .setConfig(options.getYtConf(TableWriterConfig).orNull)
      .setPath(appendPath)
      .setSerializationContext(createSerializationContext())
      .setTransactionalOptions(new TransactionalOptions(GUID.valueOf(transactionGuid)))
      .setNeedRetries(false)
      .setRequestId(requestId)
      .build()
    client.writeTableV2(request).join()
  }

  override protected def finishWriter(writer: AsyncWriter[InternalRow], timeoutMs: Long): Unit = {
    writer.finish().get(timeoutMs, TimeUnit.MILLISECONDS)
  }
}
