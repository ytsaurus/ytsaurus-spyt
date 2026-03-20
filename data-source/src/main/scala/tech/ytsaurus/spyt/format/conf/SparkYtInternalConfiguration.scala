package tech.ytsaurus.spyt.format.conf

import tech.ytsaurus.spyt.wrapper.config.ConfigEntry
import tech.ytsaurus.ysontree.YTreeNode

object SparkYtInternalConfiguration {
  import ConfigEntry.implicits._
  private val prefix = "internal"

  case object Transaction extends ConfigEntry[String](s"$prefix.transaction")

  case object GlobalTransaction extends ConfigEntry[String](s"$prefix.globalTransaction")

  // read options

  case object FullReadAllowed extends ConfigEntry[Boolean]("full_read_allowed", Some(true))

  // write options

  case object IdMapping extends ConfigEntry[Array[Int]]("id_mapping", None)

  case object BaseSchema extends ConfigEntry[YTreeNode]("base_schema", None)

}
