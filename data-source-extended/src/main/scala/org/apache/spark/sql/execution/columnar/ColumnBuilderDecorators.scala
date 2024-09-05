package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.columnar.ColumnBuilder")
object ColumnBuilderDecorators {

  @DecoratedMethod
  def apply(dataType: DataType,
            initialSize: Int,
            columnName: String,
            useCompression: Boolean): ColumnBuilder = {
    dataType match {
      case UInt64Type =>
        val builder = new LongColumnBuilder
        builder.initialize(initialSize, columnName, useCompression)
        builder
      case _ => __apply(dataType, initialSize, columnName, useCompression)
    }
  }

  def __apply(dataType: DataType,
            initialSize: Int,
            columnName: String,
            useCompression: Boolean): ColumnBuilder = ???

}
