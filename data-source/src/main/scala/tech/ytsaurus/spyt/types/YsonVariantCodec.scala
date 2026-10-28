package tech.ytsaurus.spyt.types

import tech.ytsaurus.yson.{YsonBinaryWriter, YsonConsumer, YsonParser}

import java.io.{ByteArrayOutputStream, OutputStream}

/** YSON bytes in, binary or text, and binary YSON out for a YsonVariantConverter, which works with YsonEvents. */
object YsonVariantCodec {

  // Bytes YsonBinaryWriter buffers before it copies them to the output; its 4 KiB default is too large for one cell
  private val WRITER_BUFFER_SIZE = 64

  /** Converts a whole binary or text YSON value; see YsonVariantConverter.fromYsonEvents. */
  def fromYson(converter: YsonVariantConverter, ysonBytes: Array[Byte], allowDuplicateKeys: Boolean): Any = {
    val parse = (consumer: YsonEvents) => new YsonParser(ysonBytes).parseNode(new YsonEventsForwarder(consumer))
    converter.fromYsonEvents(parse, allowDuplicateKeys)
  }

  /** Serializes a non-null VariantVal as binary YSON; see YsonVariantConverter.writeYsonEvents. */
  def toYson(converter: YsonVariantConverter, variant: Any): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new YsonEventsWriter(output)
    converter.writeYsonEvents(variant, writer)
    writer.flush()
    output.toByteArray
  }

  private class YsonEventsForwarder(consumer: YsonEvents) extends YsonConsumer {

    override def onEntity(): Unit = consumer.onEntity()

    override def onInteger(value: Long): Unit = consumer.onInteger(value)

    override def onUnsignedInteger(value: Long): Unit = consumer.onUnsignedInteger(value)

    override def onBoolean(value: Boolean): Unit = consumer.onBoolean(value)

    override def onDouble(value: Double): Unit = consumer.onDouble(value)

    override def onString(value: Array[Byte], offset: Int, length: Int): Unit = consumer.onString(value, offset, length)

    override def onBeginList(): Unit = consumer.onBeginList()

    override def onListItem(): Unit = consumer.onListItem()

    override def onEndList(): Unit = consumer.onEndList()

    override def onBeginAttributes(): Unit = consumer.onBeginAttributes()

    override def onEndAttributes(): Unit = consumer.onEndAttributes()

    override def onBeginMap(): Unit = consumer.onBeginMap()

    override def onEndMap(): Unit = consumer.onEndMap()

    override def onKeyedItem(value: Array[Byte], offset: Int, length: Int): Unit = {
      consumer.onKeyedItem(value, offset, length)
    }
  }

  /** The binary YSON writer, which already has every method of YsonEvents. */
  private class YsonEventsWriter(output: OutputStream)
    extends YsonBinaryWriter(output, WRITER_BUFFER_SIZE) with YsonEvents
}
