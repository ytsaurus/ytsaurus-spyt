package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.execution.columnar.GenerateColumnAccessorDecoratorsUtils.patchColumnTypes
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.{DataType, LongType}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.columnar.GenerateColumnAccessor")
object GenerateColumnAccessorDecorators {

  @DecoratedMethod
  protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
    __create(patchColumnTypes(columnTypes))
  }

  protected def __create(columnTypes: Seq[DataType]): ColumnarIterator = ???
}

object GenerateColumnAccessorDecoratorsUtils {
  def patchColumnTypes(columnTypes: Seq[DataType]): Seq[DataType] = columnTypes.map {
    case UInt64Type => LongType
    case other => other
  }
}