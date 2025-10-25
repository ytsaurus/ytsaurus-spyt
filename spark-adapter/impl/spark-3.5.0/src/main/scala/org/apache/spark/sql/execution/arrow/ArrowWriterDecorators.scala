package org.apache.spark.sql.execution.arrow

import org.apache.arrow.vector.{BigIntVector, LargeVarBinaryVector, UInt8Vector, ValueVector, VarBinaryVector}
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.util.ArrowUtils
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.arrow.ArrowWriter$")
@Applicability(from = "3.5.0")
object ArrowWriterDecorators {

  @DecoratedMethod
  private[sql] def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()

    (ArrowUtils.fromArrowField(field), vector) match {
      case (ts.uInt64DataType, vector: UInt8Vector) => new UInt8Writer(vector)
      case (ts.ysonDataType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (ts.ysonDataType, vector: LargeVarBinaryVector) => new LargeBinaryWriter(vector)
      case _ => __createFieldWriter(vector)
    }
  }

  private[sql] def __createFieldWriter(vector: ValueVector): ArrowFieldWriter = ???

}

private[arrow] class UInt8Writer(val valueVector: UInt8Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}
