package org.apache.spark.adapter

import org.apache.spark.internal.config.ConfigBuilder

object Config {
  val YTSAURUS_ARROW_STRING_TO_BINARY = ConfigBuilder("spark.ytsaurus.arrow.stringToBinary")
    .doc("Serialize StringType values as arrow binary type")
    .version("2.9.0")
    .booleanConf
    .createWithDefault(false)
}
