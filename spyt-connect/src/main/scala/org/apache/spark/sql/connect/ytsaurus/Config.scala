package org.apache.spark.sql.connect.ytsaurus

import org.apache.spark.internal.config.ConfigBuilder

import java.util.concurrent.TimeUnit

object Config {
  val YTSAURUS_CONNECT_IDLE_TIMEOUT = ConfigBuilder("spark.ytsaurus.connect.idle.timeout")
    .doc("Idle timeout for SPYT connect server since the last request was completed")
    .version("2.8.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("10m")

  val YTSAURUS_CONNECT_STARTUP_TIMEOUT = ConfigBuilder("spark.ytsaurus.connect.startup.timeout")
    .doc("Timeout for receiving GRPC endpoint from SPYT connect server")
    .version("2.9.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("2m")

  val YTSAURUS_CONNECT_SETTINGS_HASH = ConfigBuilder("spark.ytsaurus.connect.settings.hash")
    .doc("Configuration settings hash that serves as a key for looking up an existing instance on Spark Master")
    .version("2.9.0")
    .stringConf
    .createOptional

  val YTSAURUS_CONNECT_FORWARD_METHODS = ConfigBuilder("spark.ytsaurus.connect.forward.methods")
    .doc("Comma-separated Spark Connect gRPC methods or ExecutePlan selectors handled by an external endpoint")
    .version("2.12.0")
    .stringConf
    .createOptional

  val YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS =
    ConfigBuilder("spark.ytsaurus.connect.forward.mirror.methods")
      .doc("Comma-separated Spark Connect gRPC methods or ExecutePlan selectors handled locally and forwarded")
      .version("2.12.0")
      .stringConf
      .createOptional

  val YTSAURUS_CONNECT_FORWARD_ENDPOINT = ConfigBuilder("spark.ytsaurus.connect.forward.endpoint")
    .doc("Address of the external Spark Connect gRPC endpoint")
    .version("2.12.0")
    .stringConf
    .createOptional
}
