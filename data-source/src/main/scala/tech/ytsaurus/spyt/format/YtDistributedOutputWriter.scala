package tech.ytsaurus.spyt.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.client.{AsyncFragmentWriter, CompoundClient}
import tech.ytsaurus.client.request.{DistributedWriteCookie, WriteFragmentResult, WriteSerializationContext, WriteTableFragment}
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.TableWriterConfig
import tech.ytsaurus.spyt.serializers.{InternalRowSerializer, WriteSchemaConverter, SchemaConverter}
import tech.ytsaurus.spyt.wrapper.config._

import java.util.concurrent.TimeUnit

class YtDistributedOutputWriter(
  override val path: String,
  cookie: DistributedWriteCookie,
  schema: StructType,
  sortOption: SchemaConverter.SortOption,
  writeConfiguration: SparkYtWriteConfiguration,
  options: Map[String, String])(implicit client: CompoundClient)
  extends AbstractYtOutputWriter[AsyncFragmentWriter[InternalRow]](schema, writeConfiguration, options) {

  private var hasWrittenRows = false

  override protected def initializeWriter(): AsyncFragmentWriter[InternalRow] = {
    val serializationContext = new WriteSerializationContext(
      new InternalRowSerializer(schema, WriteSchemaConverter(options), sortOption)
    )
    val wtfRequest = WriteTableFragment.builder[InternalRow]()
      .setConfig(options.getYtConf(TableWriterConfig).orNull)
      .setCookie(cookie)
      .setSerializationContext(serializationContext)
      .setRequestId(requestId)
      .build()
    client.writeTableFragment(wtfRequest).join()
  }

  override def write(record: InternalRow): Unit = {
    hasWrittenRows = true
    super.write(record)
  }

  override protected def finishWriter(writer: AsyncFragmentWriter[InternalRow], timeoutMs: Long): Unit = {
    val result: WriteFragmentResult = writer.finish().get(timeoutMs, TimeUnit.MILLISECONDS)
    if (hasWrittenRows) {
      YtOutputCommitProtocol.setWriteFragmentResult(result)
    }
  }
}
