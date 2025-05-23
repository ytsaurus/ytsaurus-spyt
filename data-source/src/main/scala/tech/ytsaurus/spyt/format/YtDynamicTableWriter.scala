package tech.ytsaurus.spyt.format

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.ModifyRowsRequest
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered
import tech.ytsaurus.spyt.serializers.{DynTableRowConverter, WriteSchemaConverter}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.collection.JavaConverters._

class YtDynamicTableWriter(richPath: YPathEnriched,
                           schema: StructType,
                           wConfig: SparkYtWriteConfiguration,
                           options: Map[String, String])
                          (implicit ytClient: CompoundClient) extends OutputWriter {

  import YtDynamicTableWriter._

  override val path: String = richPath.toStringPath
  private val writeSchemaConverter = WriteSchemaConverter(options)
  private val typeV3: Boolean = writeSchemaConverter.typeV3Format
  private val tableSchema: TableSchema = writeSchemaConverter.tableSchema(schema, Unordered)
  private val rowConverter: DynTableRowConverter = new DynTableRowConverter(schema, tableSchema, typeV3)
  private var count = 0
  private var modifyRowsRequestBuilder: ModifyRowsRequest.Builder = _

  initialize()

  def write(row: Seq[Any]): Unit = {
    val preparedRow = rowConverter.convertRow(row)
    modifyRowsRequestBuilder.addInsert(preparedRow.asJava)
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
      YtWrapper.insertRows(modifyRowsRequestBuilder.build(), None)
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
}
