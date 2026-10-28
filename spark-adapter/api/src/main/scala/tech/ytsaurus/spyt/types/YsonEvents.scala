package tech.ytsaurus.spyt.types

trait YsonEvents {

  def onEntity(): Unit

  def onInteger(value: Long): Unit

  def onUnsignedInteger(value: Long): Unit

  def onBoolean(value: Boolean): Unit

  def onDouble(value: Double): Unit

  def onString(value: Array[Byte], offset: Int, length: Int): Unit

  def onBeginList(): Unit

  def onListItem(): Unit

  def onEndList(): Unit

  def onBeginAttributes(): Unit

  def onEndAttributes(): Unit

  def onBeginMap(): Unit

  def onEndMap(): Unit

  def onKeyedItem(value: Array[Byte], offset: Int, length: Int): Unit
}
