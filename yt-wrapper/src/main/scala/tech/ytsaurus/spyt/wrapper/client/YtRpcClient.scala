package tech.ytsaurus.spyt.wrapper.client

import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.bus.DefaultBusConnector

case class YtRpcClient(normalizedProxy: String, yt: CompoundClient, connector: DefaultBusConnector) extends AutoCloseable {
  private val log = LoggerFactory.getLogger(getClass)

  def close(): Unit = {
    log.info(s"Close YtRpcClient for normalizedProxy $normalizedProxy")
    yt.close()
    connector.close()
    log.info(s"Successfully closed YtRpcClient for normalizedProxy $normalizedProxy")
  }
}
