package tech.ytsaurus.spyt.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.client.{AsyncFragmentWriter, CompoundClient}
import tech.ytsaurus.client.request.{DistributedWriteCookie, WriteFragmentResult, WriteTableFragment}
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.TableWriterConfig
import tech.ytsaurus.spyt.wrapper.config._

import java.util.concurrent.TimeUnit

class YtDistributedOutputWriter(
  override val path: String,
  cookie: DistributedWriteCookie,
  schema: StructType,
  writeConfiguration: SparkYtWriteConfiguration,
  options: Map[String, String])(implicit client: CompoundClient)
  extends AbstractYtOutputWriter[AsyncFragmentWriter[InternalRow]](schema, writeConfiguration, options) {

  override protected def initializeWriter(): AsyncFragmentWriter[InternalRow] = {
    val wtfRequest = WriteTableFragment.builder[InternalRow]()
      .setConfig(options.getYtConf(TableWriterConfig).orNull)
      .setCookie(cookie)
      .setSerializationContext(createSerializationContext())
      .setRequestId(requestId)
      .build()
    client.writeTableFragment(wtfRequest).join()
  }

  override protected def finishWriter(writer: AsyncFragmentWriter[InternalRow], timeoutMs: Long): Unit = {
    val result: WriteFragmentResult = writer.finish().get(timeoutMs, TimeUnit.MILLISECONDS)
    YtOutputCommitProtocol.setWriteFragmentResult(result)
  }
}
