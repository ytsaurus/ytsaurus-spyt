package tech.ytsaurus.spyt.types

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Converts YTsaurus YSON values to Spark Variant values and back. Spark has Variant only from 4.0, so values are typed
 * as Any: they are org.apache.spark.unsafe.types.VariantVal instances, which Spark keeps in internal and external rows.
 * YSON comes in and goes out as YsonEvents; data-source connects them to the YSON parser and writer.
 */
trait YsonVariantConverter {

  def variantType: DataType

  /** spark.sql.variant.allowDuplicateKeys in conf, to read once per reader and pass to fromYsonEvents. */
  def variantAllowDuplicateKeys(conf: SQLConf): Boolean

  /**
   * Converts the whole YSON value that parse sends to its argument; returns null for a top-level entity without
   * attributes. A repeated map key keeps its last value when allowDuplicateKeys is true and fails like parse_json
   * otherwise.
   */
  def fromYsonEvents(parse: YsonEvents => Unit, allowDuplicateKeys: Boolean): Any

  def fromInt64(value: Long): Any

  /** Reads value as unsigned: above Long.MaxValue it becomes a decimal with scale 0, as nested uint64 values do. */
  def fromUint64(value: Long): Any

  def fromDouble(value: Double): Any

  def fromBoolean(value: Boolean): Any

  /** Valid UTF-8 becomes a Variant string, other bytes a Variant binary, as for nested YSON strings. */
  def fromString(bytes: Array[Byte]): Any

  /** Sends a non-null VariantVal to consumer as YSON events; the type mapping is documented in the Spark 4 adapter. */
  def writeYsonEvents(variant: Any, consumer: YsonEvents): Unit
}
