package org.apache.spark.sql.vectorized

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, MapType, StructType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

abstract class ColumnarBatchRowWrapperBase(
  row: InternalRow,
  getStructImpl: (Int, Int) => InternalRow,
  getArrayImpl: Int => ArrayData,
  getMapImpl: Int => MapData) extends InternalRow {
  override def numFields: Int = row.numFields

  override def setNullAt(i: Int): Unit = row.setNullAt(i)

  override def update(i: Int, value: Any): Unit = row.update(i, value)

  override def copy(): InternalRow = row.copy()

  override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = row.getByte(ordinal)

  override def getShort(ordinal: Int): Short = row.getShort(ordinal)

  override def getInt(ordinal: Int): Int = row.getInt(ordinal)

  override def getLong(ordinal: Int): Long = row.getLong(ordinal)

  override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = row.getDecimal(ordinal, precision, scale)

  override def getUTF8String(ordinal: Int): UTF8String = row.getUTF8String(ordinal)

  override def getBinary(ordinal: Int): Array[Byte] = row.getBinary(ordinal)

  override def getInterval(ordinal: Int): CalendarInterval = row.getInterval(ordinal)

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getStructImpl(ordinal, numFields)

  override def getArray(ordinal: Int): ArrayData = getArrayImpl(ordinal)

  override def getMap(ordinal: Int): MapData = getMapImpl(ordinal)

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    dataType match {
      case StructType(fields) => getStruct(ordinal, fields.length)
      case ArrayType(_, _) => getArray(ordinal)
      case MapType(_, _, _) => getMap(ordinal)
      case _ => row.get(ordinal, dataType)
    }
  }
}
