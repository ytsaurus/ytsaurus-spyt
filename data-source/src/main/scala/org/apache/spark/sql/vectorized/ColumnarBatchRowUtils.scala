package org.apache.spark.sql.vectorized

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.serialization.YsonDecoder
import tech.ytsaurus.spyt.serializers.SchemaConverter

object ColumnarBatchRowUtils {
  def unsafeProjection(schema: StructType): InternalRow => UnsafeRow = {
    val unsafeProjection = UnsafeProjection.create(schema)
    val mutableColumnarRowProjection = new ColumnarBatchRowProjection(schema)

    r => unsafeProjection.apply(mutableColumnarRowProjection.apply(r))
  }

  class ColumnarBatchRowProjection(schema: StructType) {
    private val indexedDataTypes = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))

    def apply(row: InternalRow): InternalRow = {
      val getStructImpl: (Int, Int) => InternalRow = (ordinal: Int, numFields: Int) =>
        YsonDecoder.decode(row.getBinary(ordinal), indexedDataTypes(ordinal)).asInstanceOf[InternalRow]

      val getArrayImpl: Int => ArrayData = (ordinal: Int) =>
        YsonDecoder.decode(row.getBinary(ordinal), indexedDataTypes(ordinal)).asInstanceOf[GenericArrayData]

      val getMapImpl: Int => MapData = (ordinal: Int) =>
        YsonDecoder.decode(row.getBinary(ordinal), indexedDataTypes(ordinal)).asInstanceOf[MapData]

      SparkAdapter.instance.createColumnarBatchRowWrapper(row, getStructImpl, getArrayImpl, getMapImpl)
    }
  }

}
