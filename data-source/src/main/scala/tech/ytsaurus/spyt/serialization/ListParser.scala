package tech.ytsaurus.spyt.serialization

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{StructField, StructType}
import tech.ytsaurus.yson.YsonTags

trait ListParser {
  self: YsonBaseReader =>

  private val endToken = YsonTags.END_LIST

  def parseYsonListAsSparkList(allowEof: Boolean, elementType: IndexedDataType): ArrayData = {
    val res = Array.newBuilder[Any]
    readList(endToken, allowEof) { (index, token) =>
      try {
        res += parseNode(token, allowEof, elementType)
      } catch {
        case e: Exception => throw new RuntimeException("" + index, e)
      }
    }
    ArrayData.toArrayData(res.result())
  }

  def parseYsonListAsNone(allowEof: Boolean): Int = {
    readList(endToken, allowEof) { (_, token) =>
      parseNode(token, allowEof, IndexedDataType.NoneType)
    }
    1
  }

  def parseYsonListAsSparkStruct(allowEof: Boolean, schema: IndexedDataType.StructType): InternalRow = {
    val res = new Array[Any](schema.map.size)
    readList(endToken, allowEof) { (index, token) =>
      val fieldName = schema.sparkDataType.fields(index).name
      try {
        res(index) = parseNode(token, allowEof, schema.map.getOrElse(
          fieldName, throw new NoSuchElementException(s"$fieldName is not found in schema map")
        ).dataType)
      } catch {
        case e: Exception => throw new RuntimeException(fieldName, e)
      }
    }
    new GenericInternalRow(res)
  }

  def parseYsonListAsArray(allowEof: Boolean, schema: IndexedDataType.TupleType): Array[Any] = {
    val res = new Array[Any](schema.length)
    readList(endToken, allowEof) { (index, token) =>
      try {
        if (index < schema.length) {
          res(index) = parseNode(token, allowEof, schema(index))
        } else {
          parseNode(token, allowEof, IndexedDataType.NoneType)
        }
      } catch {
        case e: Exception => throw new RuntimeException("" + index, e)
      }
    }
    res
  }

  def parseYsonListAsSparkMap(allowEof: Boolean, schema: IndexedDataType.MapType): ArrayBasedMapData = {
    var resKeys: Seq[Any] = Nil
    var resValues: Seq[Any] = Nil
    val structSchema = StructType(List(
      StructField("_1", schema.keyType.sparkDataType),
      StructField("_2", schema.valueType.sparkDataType)
    ))
    val pairType = IndexedDataType.TupleType(Seq(schema.keyType, schema.valueType), structSchema)
    readList(endToken, allowEof) { (_, _) =>
      val keyValue = parseYsonListAsArray(allowEof, pairType)
      val key = keyValue(0)
      if (key == null) unexpectedToken(YsonTags.ENTITY, "NODE")
      val value = keyValue(1)
      resKeys = key +: resKeys
      resValues = value +: resValues
    }
    new ArrayBasedMapData(ArrayData.toArrayData(resKeys), ArrayData.toArrayData(resValues))
  }

  def parseYsonListAsSparkTuple(allowEof: Boolean, schema: IndexedDataType.TupleType): InternalRow = {
    new GenericInternalRow(parseYsonListAsArray(allowEof, schema))
  }

}
