package tech.ytsaurus.spyt.variant

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, VariantType}
import org.apache.spark.types.variant.{Variant, VariantBuilder, VariantUtil}
import org.apache.spark.types.variant.VariantBuilder.FieldEntry
import org.apache.spark.unsafe.types.VariantVal

import tech.ytsaurus.spyt.types.{YsonEvents, YsonVariantConverter}

import java.math.BigDecimal
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, ZoneOffset}
import java.util

/**
 * Converts YSON to Spark Variant and back. A node with attributes, which Variant cannot express, becomes an object
 * with the keys ATTRIBUTES_KEY (the attributes as an object) and VALUE_KEY (the node itself). As in the YTsaurus JSON
 * format, a map or attribute key that starts with KEY_ESCAPE gets one more KEY_ESCAPE, so no YSON map reads as such an
 * object. Variant keys are strings, so a key that is not valid UTF-8 is an error.
 */
object YsonVariantConverter400 extends YsonVariantConverter {

  private val ATTRIBUTES_KEY = "$attributes"
  private val VALUE_KEY = "$value"
  private val KEY_ESCAPE = "$"
  private val ESCAPED_KEY_PREFIX = KEY_ESCAPE + KEY_ESCAPE

  // An error message shows at most this many bytes of a key
  private val MAX_KEY_BYTES_IN_MESSAGE = 32

  private val REPLACEMENT_CHAR = '\uFFFD'

  override def variantType: DataType = VariantType

  override def variantAllowDuplicateKeys(conf: SQLConf): Boolean = conf.getConf(SQLConf.VARIANT_ALLOW_DUPLICATE_KEYS)

  override def fromYsonEvents(parse: YsonEvents => Unit, allowDuplicateKeys: Boolean): Any = {
    val consumer = new YsonToVariantConsumer(allowDuplicateKeys)
    parse(consumer)
    consumer.result()
  }

  override def fromInt64(value: Long): Any = buildScalar(_.appendLong(value))

  override def fromUint64(value: Long): Any = buildScalar(appendUint64(_, value))

  override def fromDouble(value: Double): Any = buildScalar(_.appendDouble(value))

  override def fromBoolean(value: Boolean): Any = buildScalar(_.appendBoolean(value))

  override def fromString(bytes: Array[Byte]): Any = buildScalar(appendYsonString(_, bytes, 0, bytes.length))

  /**
   * Writes Spark Variant as YSON events:
   *  - null, booleans, integers, doubles, strings, arrays and objects become the same YSON types, and floats
   *    become doubles;
   *  - a decimal with original scale 0 becomes an int64, or a uint64 when it is above Long.MaxValue but fits 64 bits;
   *    any other decimal becomes a double;
   *  - binary becomes a YSON string of the same bytes;
   *  - date, timestamp, timestamp_ntz and uuid become strings in the to_json format, with timestamps in UTC;
   *  - a nested object with exactly two keys, ATTRIBUTES_KEY with an object value and VALUE_KEY, becomes a node with
   *    attributes. Such an object is an error at the top level, because YTsaurus table values cannot have top-level
   *    attributes, and as the VALUE_KEY of another such object, because a node has at most one attribute map;
   *  - an object key that starts with ESCAPED_KEY_PREFIX loses one KEY_ESCAPE. Other keys stay as they are, so a key
   *    with one KEY_ESCAPE reads back with two. An object with both $x and $$x, which would both become the YSON key
   *    $x, is an error.
   */
  override def writeYsonEvents(variant: Any, consumer: YsonEvents): Unit = variant match {
    case value: VariantVal => write(new Variant(value.getValue, value.getMetadata), consumer, attributesAllowed = false)
    case other => throw new IllegalArgumentException(s"Only a non-null VariantVal can be converted to YSON, got $other")
  }

  private def toVariantVal(variant: Variant): VariantVal = {
    new VariantVal(variant.getValue, variant.getMetadata)
  }

  /** Spark has no unsigned integers: a uint64 above Long.MaxValue becomes a decimal with scale 0. */
  private def appendUint64(builder: VariantBuilder, value: Long): Unit = {
    if (value >= 0) {
      builder.appendLong(value)
    } else {
      builder.appendDecimal(new BigDecimal(java.lang.Long.toUnsignedString(value)))
    }
  }

  /** YSON strings are bytes: valid UTF-8 becomes a Variant string, anything else a Variant binary. */
  private def appendYsonString(builder: VariantBuilder, bytes: Array[Byte], offset: Int, length: Int): Unit = {
    decodeUtf8(bytes, offset, length) match {
      case Some(string) => builder.appendString(string)
      case None => builder.appendBinary(util.Arrays.copyOfRange(bytes, offset, offset + length))
    }
  }

  /** The Variant key of a YSON map or attribute key. */
  private def variantKey(bytes: Array[Byte], offset: Int, length: Int): String = {
    val key = decodeUtf8(bytes, offset, length).getOrElse {
      throw new IllegalArgumentException(
        s"YSON key ${keyForMessage(bytes, offset, length)} is not valid UTF-8, so it cannot be a Variant object key.")
    }
    if (key.startsWith(KEY_ESCAPE)) KEY_ESCAPE + key else key
  }

  /** Decodes bytes that are valid UTF-8; None for any other bytes. */
  private def decodeUtf8(bytes: Array[Byte], offset: Int, length: Int): Option[String] = {
    val string = new String(bytes, offset, length, StandardCharsets.UTF_8)
    // new String puts REPLACEMENT_CHAR in place of invalid bytes, so only a string that has it needs the strict check
    if (string.indexOf(REPLACEMENT_CHAR) < 0 || isValidUtf8(bytes, offset, length)) Some(string) else None
  }

  private def isValidUtf8(bytes: Array[Byte], offset: Int, length: Int): Boolean = {
    // UTF-8 decodes to at most one char per byte
    val chars = CharBuffer.allocate(length)
    !StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes, offset, length), chars, true).isError
  }

  /** The key in YSON text with \xHH escapes, cut to MAX_KEY_BYTES_IN_MESSAGE bytes. */
  private def keyForMessage(bytes: Array[Byte], offset: Int, length: Int): String = {
    val text = (offset until offset + math.min(length, MAX_KEY_BYTES_IN_MESSAGE)).map { i =>
      val byte = bytes(i) & 0xff
      if (byte >= 0x20 && byte < 0x7f && byte != '"' && byte != '\\') byte.toChar.toString else f"\\x$byte%02X"
    }.mkString
    val quoted = "\"" + text + "\""
    if (length > MAX_KEY_BYTES_IN_MESSAGE) s"$quoted... ($length bytes)" else quoted
  }

  private def buildScalar(append: VariantBuilder => Unit): VariantVal = {
    val builder = new VariantBuilder(false)
    append(builder)
    toVariantVal(builder.result())
  }

  private def write(variant: Variant, consumer: YsonEvents, attributesAllowed: Boolean): Unit = {
    variant.getType match {
      case VariantUtil.Type.NULL => consumer.onEntity()
      case VariantUtil.Type.BOOLEAN => consumer.onBoolean(variant.getBoolean)
      case VariantUtil.Type.LONG => consumer.onInteger(variant.getLong)
      case VariantUtil.Type.DECIMAL => writeDecimal(variant, consumer)
      case VariantUtil.Type.DOUBLE => consumer.onDouble(variant.getDouble)
      case VariantUtil.Type.FLOAT => consumer.onDouble(variant.getFloat)
      case VariantUtil.Type.STRING => writeString(variant.getString, consumer)
      case VariantUtil.Type.BINARY =>
        val bytes = variant.getBinary
        consumer.onString(bytes, 0, bytes.length)
      case VariantUtil.Type.DATE => writeString(LocalDate.ofEpochDay(variant.getLong).toString, consumer)
      case VariantUtil.Type.TIMESTAMP | VariantUtil.Type.TIMESTAMP_NTZ =>
        // to_json quotes these values and has nothing to escape in them
        val json = variant.toJson(ZoneOffset.UTC)
        writeString(json.substring(1, json.length - 1), consumer)
      case VariantUtil.Type.UUID => writeString(variant.getUuid.toString, consumer)
      case VariantUtil.Type.ARRAY =>
        consumer.onBeginList()
        for (i <- 0 until variant.arraySize) {
          consumer.onListItem()
          write(variant.getElementAtIndex(i), consumer, attributesAllowed = true)
        }
        consumer.onEndList()
      case VariantUtil.Type.OBJECT if isNodeWithAttributes(variant) =>
        if (!attributesAllowed) {
          throw new IllegalArgumentException(s"A Variant object with exactly the keys $ATTRIBUTES_KEY and " +
            s"$VALUE_KEY stands for a YSON node with attributes, which cannot be a top-level value or the value of " +
            s"another node with attributes. To write it as a map, name its keys ${KEY_ESCAPE + ATTRIBUTES_KEY} and " +
            s"${KEY_ESCAPE + VALUE_KEY}.")
        }
        consumer.onBeginAttributes()
        writeFields(variant.getFieldByKey(ATTRIBUTES_KEY), consumer)
        consumer.onEndAttributes()
        // A YSON node has at most one attribute map
        write(variant.getFieldByKey(VALUE_KEY), consumer, attributesAllowed = false)
      case VariantUtil.Type.OBJECT =>
        consumer.onBeginMap()
        writeFields(variant, consumer)
        consumer.onEndMap()
      case otherType =>
        throw new UnsupportedOperationException(s"Variant values of type $otherType cannot be converted to YSON")
    }
  }

  private def writeFields(variant: Variant, consumer: YsonEvents): Unit = {
    // The YSON keys written so far that start with KEY_ESCAPE, with their Variant keys: both $x and $$x give $x
    var variantKeysByYsonKey: util.HashMap[String, String] = null
    for (i <- 0 until variant.objectSize) {
      val field = variant.getFieldAtIndex(i)
      val key = ysonKey(field.key)
      if (key.startsWith(KEY_ESCAPE)) {
        if (variantKeysByYsonKey == null) {
          variantKeysByYsonKey = new util.HashMap[String, String]()
        }
        val otherVariantKey = variantKeysByYsonKey.put(key, field.key)
        if (otherVariantKey != null) {
          throw new IllegalArgumentException(s"""Variant object keys "$otherVariantKey" and "${field.key}" both """ +
            s"""become the YSON key "$key", because a key that starts with $ESCAPED_KEY_PREFIX loses one """ +
            s"$KEY_ESCAPE. Rename or remove one of them.")
        }
      }
      writeKey(key, consumer)
      write(field.value, consumer, attributesAllowed = true)
    }
  }

  /** The YSON key of a Variant object key, the inverse of variantKey. */
  private def ysonKey(key: String): String = {
    if (key.startsWith(ESCAPED_KEY_PREFIX)) key.substring(KEY_ESCAPE.length) else key
  }

  private def isNodeWithAttributes(variant: Variant): Boolean = {
    variant.objectSize == 2 && variant.getFieldByKey(VALUE_KEY) != null &&
      Option(variant.getFieldByKey(ATTRIBUTES_KEY)).exists(_.getType == VariantUtil.Type.OBJECT)
  }

  private def writeDecimal(variant: Variant, consumer: YsonEvents): Unit = {
    // getDecimal strips trailing zeros, which would turn 5.0 into the integer 5
    val decimal = VariantUtil.getDecimalWithOriginalScale(variant.getValue, 0)
    val unscaled = decimal.unscaledValue
    if (decimal.scale == 0 && unscaled.bitLength < java.lang.Long.SIZE) {
      consumer.onInteger(unscaled.longValue)
    } else if (decimal.scale == 0 && unscaled.signum > 0 && unscaled.bitLength == java.lang.Long.SIZE) {
      // The low 64 bits are the uint64 value, which YSON consumers take as a long
      consumer.onUnsignedInteger(unscaled.longValue)
    } else {
      consumer.onDouble(decimal.doubleValue)
    }
  }

  private def writeString(value: String, consumer: YsonEvents): Unit = {
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    consumer.onString(bytes, 0, bytes.length)
  }

  private def writeKey(key: String, consumer: YsonEvents): Unit = {
    val bytes = key.getBytes(StandardCharsets.UTF_8)
    consumer.onKeyedItem(bytes, 0, bytes.length)
  }

  private class YsonToVariantConsumer(allowDuplicateKeys: Boolean) extends YsonEvents {
    import YsonToVariantConsumer.{AttrWrapperFrame, Frame, ListFrame, MapFrame, ObjectFrame}

    private val builder = new VariantBuilder(allowDuplicateKeys)
    private val stack = new util.ArrayDeque[Frame]()
    private var isBareTopLevelEntity = false

    def result(): VariantVal = if (isBareTopLevelEntity) null else toVariantVal(builder.result())

    override def onEntity(): Unit = {
      if (stack.isEmpty) {
        isBareTopLevelEntity = true
      }
      beforeValue()
      builder.appendNull()
      afterValue()
    }

    override def onInteger(value: Long): Unit = {
      beforeValue()
      builder.appendLong(value)
      afterValue()
    }

    override def onUnsignedInteger(value: Long): Unit = {
      beforeValue()
      appendUint64(builder, value)
      afterValue()
    }

    override def onBoolean(value: Boolean): Unit = {
      beforeValue()
      builder.appendBoolean(value)
      afterValue()
    }

    override def onDouble(value: Double): Unit = {
      beforeValue()
      builder.appendDouble(value)
      afterValue()
    }

    override def onString(value: Array[Byte], offset: Int, length: Int): Unit = {
      beforeValue()
      appendYsonString(builder, value, offset, length)
      afterValue()
    }

    override def onBeginList(): Unit = {
      beforeValue()
      stack.push(new ListFrame(builder.getWritePos))
    }

    override def onListItem(): Unit = ()

    override def onEndList(): Unit = {
      val list = stack.pop().asInstanceOf[ListFrame]
      builder.finishWritingArray(list.start, list.offsets)
      afterValue()
    }

    override def onBeginMap(): Unit = {
      beforeValue()
      stack.push(new MapFrame(builder.getWritePos))
    }

    override def onKeyedItem(value: Array[Byte], offset: Int, length: Int): Unit = {
      stack.peek().asInstanceOf[MapFrame].pendingKey = variantKey(value, offset, length)
    }

    override def onEndMap(): Unit = {
      val map = stack.pop().asInstanceOf[MapFrame]
      builder.finishWritingObject(map.start, map.fields)
      afterValue()
    }

    override def onBeginAttributes(): Unit = {
      beforeValue()
      val wrapper = new AttrWrapperFrame(builder.getWritePos)
      stack.push(wrapper)
      addField(wrapper, ATTRIBUTES_KEY)
      stack.push(new MapFrame(builder.getWritePos))
    }

    override def onEndAttributes(): Unit = {
      val attributes = stack.pop().asInstanceOf[MapFrame]
      builder.finishWritingObject(attributes.start, attributes.fields)
      val wrapper = stack.peek().asInstanceOf[AttrWrapperFrame]
      addField(wrapper, VALUE_KEY)
      wrapper.valueInProgress = true
    }

    private def addField(frame: ObjectFrame, key: String): Unit = {
      frame.fields.add(new FieldEntry(key, builder.addKey(key), builder.getWritePos - frame.start))
    }

    private def beforeValue(): Unit = stack.peek() match {
      case null | _: AttrWrapperFrame =>
      case list: ListFrame =>
        list.offsets.add(Integer.valueOf(builder.getWritePos - list.start))
      case map: MapFrame =>
        addField(map, map.pendingKey)
        map.pendingKey = null
    }

    private def afterValue(): Unit = {
      var top = stack.peek()
      while (top.isInstanceOf[AttrWrapperFrame] && top.asInstanceOf[AttrWrapperFrame].valueInProgress) {
        val wrapper = stack.pop().asInstanceOf[AttrWrapperFrame]
        builder.finishWritingObject(wrapper.start, wrapper.fields)
        top = stack.peek()
      }
    }
  }

  private object YsonToVariantConsumer {

    private sealed trait Frame

    private final class ListFrame(val start: Int) extends Frame {
      val offsets = new util.ArrayList[Integer]()
    }

    private sealed abstract class ObjectFrame(val start: Int) extends Frame {
      val fields = new util.ArrayList[FieldEntry]()
    }

    private final class MapFrame(start: Int) extends ObjectFrame(start) {
      var pendingKey: String = _
    }

    /** The object that stands for a node with attributes; valueInProgress is set once the attributes are written. */
    private final class AttrWrapperFrame(start: Int) extends ObjectFrame(start) {
      var valueInProgress: Boolean = false
    }
  }
}
