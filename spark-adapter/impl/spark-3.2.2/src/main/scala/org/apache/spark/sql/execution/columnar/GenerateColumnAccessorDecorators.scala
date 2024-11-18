package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.execution.columnar.GenerateColumnAccessorDecoratorsUtils.patchColumnTypes
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import org.apache.spark.sql.types.{DataType, LongType}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.columnar.GenerateColumnAccessor$")
object GenerateColumnAccessorDecorators {

  @DecoratedMethod
  protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
    __create(patchColumnTypes(columnTypes))
  }

  protected def __create(columnTypes: Seq[DataType]): ColumnarIterator = ???
}

object GenerateColumnAccessorDecoratorsUtils {
  def patchColumnTypes(columnTypes: Seq[DataType]): Seq[DataType] = columnTypes.map {
    case ts.uInt64DataType => LongType
    case other => other
  }
}