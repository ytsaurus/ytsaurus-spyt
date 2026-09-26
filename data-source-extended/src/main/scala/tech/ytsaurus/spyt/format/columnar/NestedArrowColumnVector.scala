package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.spark.sql.YtColumnarFunctionRegistration
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Collections

import scala.jdk.CollectionConverters._

/** Borrows nested Arrow vectors containing UInt64, recursively selecting SPYT's unsigned leaf accessor.
 *
 * Created by the columnar executor because Spark's nested Arrow wrappers construct unsupported unsigned
 * leaf wrappers internally. Closing this view leaves all buffers with the owning output root.
 */
private[columnar] class NestedArrowColumnVector(vector: FieldVector)
  extends ColumnVector(YtColumnarFunctionRegistration.fromArrowSchema(
    new Schema(Collections.singletonList(vector.getField))).fields.head.dataType) {

  private val children = vector.getChildrenFromFields.asScala.iterator.map(YtColumnarUdfExec.borrowedColumn).toArray

  /** Exposes the borrowed vector so subsequent batch stages can retain or copy its Arrow buffers. */
  def getValueVector: FieldVector = vector

  override def close(): Unit = ()
  override def hasNull: Boolean = vector.getNullCount > 0
  override def numNulls(): Int = vector.getNullCount
  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)
  override def getChild(ordinal: Int): ColumnVector = children(ordinal)

  override def getArray(rowId: Int): ColumnarArray = {
    val list = vector.asInstanceOf[ListVector]
    val start = list.getElementStartIndex(rowId)
    new ColumnarArray(children(0), start, list.getElementEndIndex(rowId) - start)
  }

  override def getMap(rowId: Int): ColumnarMap = {
    val map = vector.asInstanceOf[MapVector]
    val start = map.getElementStartIndex(rowId)
    new ColumnarMap(children(0).getChild(0), children(0).getChild(1), start, map.getElementEndIndex(rowId) - start)
  }

  private def unsupported: Nothing = {
    throw new UnsupportedOperationException(s"Primitive access is not supported for ${vector.getField.getType}")
  }

  override def getBoolean(rowId: Int): Boolean = unsupported
  override def getByte(rowId: Int): Byte = unsupported
  override def getShort(rowId: Int): Short = unsupported
  override def getInt(rowId: Int): Int = unsupported
  override def getLong(rowId: Int): Long = unsupported
  override def getFloat(rowId: Int): Float = unsupported
  override def getDouble(rowId: Int): Double = unsupported
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = unsupported
  override def getUTF8String(rowId: Int): UTF8String = unsupported
  override def getBinary(rowId: Int): Array[Byte] = unsupported
}

private[columnar] object NestedArrowColumnVector {

  /** Limits the custom wrapper to nested vectors whose unsigned descendants Spark cannot wrap. */
  def required(vector: FieldVector): Boolean = {
    def containsUnsigned(field: Field): Boolean = {
      field.getType == new ArrowType.Int(64, false) || field.getChildren.asScala.exists(containsUnsigned)
    }
    (vector.isInstanceOf[StructVector] || vector.isInstanceOf[ListVector]) && containsUnsigned(vector.getField)
  }
}
