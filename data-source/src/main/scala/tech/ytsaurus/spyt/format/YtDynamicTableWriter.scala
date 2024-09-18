package tech.ytsaurus.spyt.format

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.ModifyRowsRequest
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered
import tech.ytsaurus.spyt.serializers.WriteSchemaConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.collection.JavaConverters._

class YtDynamicTableWriter(richPath: YPathEnriched,
                           schema: StructType,
                           wConfig: SparkYtWriteConfiguration,
                           options: Map[String, String])
                          (implicit ytClient: CompoundClient) extends OutputWriter {
  import YtDynamicTableWriter._

  override val path: String = richPath.toStringPath

  private val writeSchemaConverter = new WriteSchemaConverter(options)

  private val tableSchema = writeSchemaConverter.tableSchema(schema, Unordered)
  private var count = 0
  private var modifyRowsRequestBuilder: ModifyRowsRequest.Builder = _

  initialize()

  def write(row: Seq[Any]): Unit = {
    modifyRowsRequestBuilder.addInsert(row.map(toPrimitives).asJava)
    count += 1
    if (count == wConfig.dynBatchSize) {
      commitBatch()
    }
  }

  override def write(row: InternalRow): Unit = write(row.toSeq(schema))

  override def close(): Unit = {
    log.debug("Closing writer")
    if (count > 0) {
      commitBatch()
    }
  }

  private def initBatch(): Unit = {
    modifyRowsRequestBuilder = ModifyRowsRequest.builder().setPath(richPath.toStringYPath).setSchema(tableSchema)
    count = 0
  }

  private def commitBatch(): Unit = {
    log.debug(s"Batch size: ${wConfig.dynBatchSize}, actual batch size: $count")
    YtMetricsRegister.time(writeBatchTime, writeBatchTimeSum) {
      val request: ModifyRowsRequest = modifyRowsRequestBuilder.build()
      val transaction = YtWrapper.createTransaction(parent = None, timeout = wConfig.timeout, sticky = true)

      transaction.modifyRows(request).join()
      transaction.commit().join()
    }
    initBatch()
  }

  private def initialize(): Unit = {
    log.debug(s"[${Thread.currentThread().getName}] Creating new YtDynamicTableWriter for path $path")
    initBatch()
    YtMetricsRegister.register()
  }
}

object YtDynamicTableWriter {
  private val log = LoggerFactory.getLogger(getClass)

  private def toPrimitives(value: Any): Any = value match {
      case v: UTF8String => v.getBytes
      case _ => value
  }
}
