package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import tech.ytsaurus.core.tables.{ColumnSchema, TableSchema}
import tech.ytsaurus.spyt.serializers.YsonRowConverter.getMapData
import tech.ytsaurus.ysontree.{YTree, YTreeListNode}

import scala.collection.JavaConverters._

class DynTableRowConverter(schema: StructType, tableSchema: TableSchema, typeV3: Boolean) {
  def convertRow(row: Seq[Any]): Seq[Any] = {
    val sparkSchemaFields = schema.fields
    val tableSchemaColumns: java.util.List[ColumnSchema] = tableSchema.getColumns

    require(row.length == sparkSchemaFields.length, "row.length != sparkSchemaFields.length")
    require(row.length == tableSchemaColumns.size(), "row.length != tableSchemaColumns.size()")

    row.zip(sparkSchemaFields.map(_.dataType)).zipWithIndex.map {
      case ((value, dataType), index) =>
        val ytType = YtTypeHolder(tableSchemaColumns.get(index).getTypeV3)
        convertField(value, dataType, typeV3, ytType)
    }
  }

  private def convertField(value: Any, dataType: DataType, typeV3: Boolean = false, ytType: YtTypeHolder = YtTypeHolder.empty): Any = {
    dataType match {
      case ArrayType(elementType, _) => convertArray(value, elementType, typeV3)
      case mapType: MapType => mapToYTreeListNode(value, mapType, typeV3)
      case structType: StructType => structToYTreeListNode(value, structType, ytType, typeV3)
      case _ =>
        value match {
          case v: UTF8String => v.getBytes
          case _ => value
        }
    }
  }

  private def convertArray(value: Any, elementType: DataType, typeV3: Boolean = false): Any = {
    value match {
      case uad: UnsafeArrayData =>
        val arrayData = uad.toArray[AnyRef](elementType).toSeq
        arrayData.map(el => convertField(el, elementType, typeV3)).asJava
      case _ => value
    }
  }

  private def mapToYTreeListNode(value: Any, mapType: MapType, typeV3: Boolean): YTreeListNode = {
    val map: Iterable[(Any, Any)] = getMapData(value, mapType.keyType, mapType.valueType)
    val convertedMap: Iterable[(Any, Any)] = if (typeV3) {
      convertMapTypeV3(map, mapType)
    } else {
      convertMapTypeV1(map, mapType)
    }

    val listBuilder = YTree.listBuilder
    convertedMap.foreach { case (key, value) =>
      listBuilder.value(YTree.listBuilder.value(key).value(value).buildList)
    }
    listBuilder.buildList
  }

  private def structToYTreeListNode(value: Any, structType: StructType, ytType: YtTypeHolder, typeV3: Boolean): YTreeListNode = {
    val indexedFields = structType.zipWithIndex

    def genHints(f: (StructField, Int) => YtTypeHolder): Seq[(StructField, Int, YtTypeHolder)] = {
      indexedFields.map { case (field, index) => (field, index, f(field, index)) }
    }

    val indexedFieldsWithHints = {
      if (ytType.supportsSearchByName) genHints((f, _) => ytType.getByName(f.name))
      else if (ytType.supportsSearchByIndex) genHints((_, i) => ytType.getByIndex(i))
      else genHints((_, _) => YtTypeHolder.empty)
    }

    val convertedStruct = indexedFieldsWithHints.map { case (field, index, hint) =>
      value match {
        case row: Row => convertField(row.get(index), field.dataType, typeV3, hint)
        case row: InternalRow => convertField(row.get(index, field.dataType), field.dataType, typeV3, hint)
        case row: UnsafeRow => convertField(row.get(index, field.dataType), field.dataType, typeV3, hint)
      }
    }

    val listBuilder = YTree.listBuilder
    convertedStruct.foreach(v => listBuilder.value(v))
    listBuilder.buildList
  }

  private def convertMapTypeV3(mapIterable: Iterable[(Any, Any)], mapType: MapType): Iterable[(Any, Any)] = {
    if (!mapType.valueContainsNull) {
      mapIterable.foreach { case (_, value) =>
        if (value == null) {
          throw new IllegalArgumentException("Try to write null value to non-null column")
        }
      }
    }
    mapIterable.map { case (key, value) =>
      val convertedKey = convertField(key, mapType.keyType)
      val convertedValue = convertField(value, mapType.valueType)
      (convertedKey, convertedValue)
    }
  }

  private def convertMapTypeV1(mapIterable: Iterable[(Any, Any)], mapType: MapType): Iterable[(Any, Any)] = {
    mapIterable.map { case (key, value) =>
      val convertedValue = convertField(value, mapType.valueType)
      (key, convertedValue)
    }
  }
}
