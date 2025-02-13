package tech.ytsaurus.spyt.format

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetricUpdater
import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings._
import tech.ytsaurus.spyt.wrapper.config._
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.{InternalRowSerializer, WriteSchemaConverter}
import tech.ytsaurus.spyt.wrapper.LogLazy
import tech.ytsaurus.client.request.{TransactionalOptions, WriteSerializationContext, WriteTable}
import tech.ytsaurus.client.{CompoundClient, TableWriter}
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class YtOutputWriter(richPath: YPathEnriched,
                     schema: StructType,
                     writeConfiguration: SparkYtWriteConfiguration,
                     options: Map[String, String])
                    (implicit client: CompoundClient) extends OutputWriter with LogLazy {

  import writeConfiguration._

  private val log = LoggerFactory.getLogger(getClass)

  private val splitPartitions = options.ytConf(SortColumns).isEmpty

  private var writers = Seq(initializeWriter())
  private var writeFutures = Seq.empty[CompletableFuture[Void]]
  private var prevFuture: Option[Future[Unit]] = None

  private var list = new util.ArrayList[InternalRow](miniBatchSize)
  private var count = 0L
  private var batchCount = 0L

  private val taskContext = TaskContext.get()

  val path: String = richPath.toStringPath

  private val reportBytesWritten: TableWriter[_] => Unit = {
    val streamWriterImpl = Class.forName("tech.ytsaurus.client.StreamWriterImpl")
    val field = streamWriterImpl.getDeclaredField("writePosition")
    writer => {
      if (streamWriterImpl.isInstance(writer)) {
        field.setAccessible(true)
        val result = field.get(streamWriterImpl.cast(writer)).asInstanceOf[Long]
        field.setAccessible(false)
        TaskMetricUpdater.reportBytesWritten(taskContext, result)
      }
    }
  }

  initialize()

  private def transactionGuid: String = richPath.transaction.get

  override def write(record: InternalRow): Unit = {
    try {
      YtMetricsRegister.time(writeTime, writeTimeSum) {
        count += 1
        batchCount += 1
        list.add(record.copy())
        if (count == miniBatchSize) {
          writeMiniBatch()
          list = new util.ArrayList[InternalRow](miniBatchSize)
          count = 0
        }
        if (batchCount == batchSize && splitPartitions) {
          writeBatch()
          batchCount = 0
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("Write failed, closing writer")
        closeWriters()
        log.warn("Write failed, writer closed")
        throw e
    }
  }

  private def closeCurrentWriter(): Unit = {
    val closePrev = prevFuture.map(f => Try(Await.result(f, timeout)))
    val currentWriter = writers.head
    writeFutures = currentWriter.readyEvent().thenComposeAsync((unused) => {
      currentWriter.close().thenAccept(unused =>
        reportBytesWritten(currentWriter)
      )
    }) +: writeFutures
    closePrev.foreach {
      case Failure(exception) =>
        throw new IllegalStateException("Yt writer is not closed properly", exception)
      case _ => // ok
    }
  }

  private def writeBatch(): Unit = {
    log.debugLazy(s"Batch of size $batchSize")
    closeCurrentWriter()
    writers = initializeWriter() +: writers
    log.debugLazy(s"Batch written")
  }

  private def writeMiniBatch(): Unit = {
    log.debugLazy(s"Writing mini batch of size $miniBatchSize")
    YtMetricsRegister.time(writeBatchTime, writeBatchTimeSum) {
      prevFuture.foreach(Await.result(_, timeout))
      prevFuture = Some(InternalRowSerializer.writeRows(writers.head, list, timeout))
    }
    log.debugLazy(s"Mini batch written")
  }

  private def closeWriters(): Unit = {
    log.debugLazy("Close writer")
    YtMetricsRegister.time(writeCloseTime, writeCloseTimeSum) {
      val currentClose = Try(closeCurrentWriter())
      val prevClose = writeFutures.map(f => Try(f.get(timeout.toMillis, TimeUnit.MILLISECONDS)))

      (currentClose +: prevClose).collectFirst {
        case Failure(exception) =>
          throw new IllegalStateException("Yt writer is not closed properly", exception)
      }
    }
    log.debugLazy("Writer closed")
  }

  override def close(): Unit = {
    log.debugLazy("Closing YtOutputWriter")
    YtMetricsRegister.time(writeTime, writeTimeSum) {
      try {
        if (count != 0) {
          log.debugLazy(s"Writing last batch, list size: ${list.size()}, writer: $this ")
          writeMiniBatch()
        }
      } finally {
        closeWriters()
      }
    }
  }

  protected def initializeWriter(): TableWriter[InternalRow] = {
    val appendPath = richPath.withAttr("append", "true").toYPath
    log.debugLazy(s"Initialize new write: $appendPath, transaction: $transactionGuid")
    val request = WriteTable.builder[InternalRow]()
      .setConfig(options.ytConf(WriteTableConfig))
      .setPath(appendPath)
      .setSerializationContext(new WriteSerializationContext(new InternalRowSerializer(schema, WriteSchemaConverter(options))))
      .setTransactionalOptions(new TransactionalOptions(GUID.valueOf(transactionGuid)))
      .setNeedRetries(false)
      .build()
    client.writeTable(request).join()
  }

  protected def initialize(): Unit = {
    YtMetricsRegister.register()
  }
}
