package tech.ytsaurus.spyt.format.conf

import tech.ytsaurus.spyt.wrapper.config.ConfigEntry

object SparkYtInternalConfiguration {
  import ConfigEntry.implicits._
  private val prefix = "internal"

  case object Transaction extends ConfigEntry[String](s"$prefix.transaction")

  case object GlobalTransaction extends ConfigEntry[String](s"$prefix.globalTransaction")

  // read options

  case object FullReadAllowed extends ConfigEntry[Boolean]("full_read_allowed", Some(true))

}
