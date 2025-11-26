package org.apache.spark.sql.connect.ytsaurus

import org.apache.spark.internal.config.ConfigBuilder

import java.util.concurrent.TimeUnit

object Config {
  val YTSAURUS_CONNECT_IDLE_TIMEOUT = ConfigBuilder("spark.ytsaurus.connect.idle.timeout")
    .doc("Idle timeout for SPYT connect server since the last request was completed")
    .version("2.8.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("10m")

  val YTSAURUS_CONNECT_TOKEN_REFRESH_PERIOD = ConfigBuilder("spark.ytsaurus.connect.token.refresh.period")
    .doc("Refresh period for temporary token specified in YT_TOKEN env variable")
    .version("2.8.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createOptional
}
