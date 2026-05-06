package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.spyt.serializers.YsonRowConverter.getMapData
import tech.ytsaurus.spyt.types.{UInt64Long, YTsaurusTypes}
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeListNode}

import scala.jdk.CollectionConverters._

class DynTableRowConverter(schema: StructType, tableSchema: TableSchema, typeV3: Boolean) {
  private val sparkSchemaFields = schema.fields
  private val tableSchemaColumns = tableSchema.getColumns.asScala.toIndexedSeq
  private val sparkFieldNames = sparkSchemaFields.map(_.name)
  private val ytColumnNames: Set[String] = tableSchemaColumns.map(_.getName).toSet
  private val sparkFieldsMissingInYtSchema = sparkFieldNames.filterNot(ytColumnNames.contains)
  require(
    sparkFieldsMissingInYtSchema.isEmpty,
    s"Cannot find YT columns for Spark fields: ${sparkFieldsMissingInYtSchema.mkString("[", ", ", "]")}. " +
      s"YT schema columns: ${ytColumnNames.toSeq.sorted.mkString("[", ", ", "]")}"
  )

  private val sparkFieldIndexByName: Map[String, Int] = sparkSchemaFields.zipWithIndex.map {
    case (field, index) => field.name -> index
  }.toMap

  private val columnConverters: IndexedSeq[Seq[Any] => Any] = tableSchemaColumns.map { columnSchema =>
    sparkFieldIndexByName.get(columnSchema.getName) match {
      case Some(sparkIndex) =>
        val dataType = sparkSchemaFields(sparkIndex).dataType
        val ytType = YtTypeHolder(columnSchema.getTypeV3)
        (row: Seq[Any]) => convertField(row(sparkIndex), dataType, typeV3, ytType)
      case None if columnSchema.getSortOrder != null =>
        throw new IllegalArgumentException(
          s"Cannot find Spark field for YT key column '${columnSchema.getName}'. " +
            s"Spark schema fields: ${sparkFieldNames.mkString("[", ", ", "]")}"
        )
      case None =>
        (_: Seq[Any]) => null
    }
  }

  def convertRow(row: Seq[Any]): Seq[Any] = {
    columnConverters.map(_(row))
  }

  private def convertField(value: Any, dataType: DataType, typeV3: Boolean = false, ytType: YtTypeHolder = YtTypeHolder.empty): Any = {
    dataType match {
      case ArrayType(elementType, _) => convertArray(value, elementType, typeV3, ytType)
      case mapType: MapType => mapToYTreeListNode(value, mapType, typeV3, ytType)
      case structType: StructType => structToYTreeListNode(value, structType, ytType, typeV3)
      case _ if value != null &&
        (DynTableRowConverter.isIntegerLikeType(dataType) ||
          DynTableRowConverter.isUInt64DataType(dataType) ||
          value.isInstanceOf[UInt64Long]) &&
        DynTableRowConverter.isUnsignedInt(ytType) =>
        YTree.unsignedLongNode(DynTableRowConverter.toUnsignedLong(value))
      case _ =>
        value match {
          case v: UTF8String => v.getBytes
          case _ => value
        }
    }
  }

  private def convertArray(value: Any, elementType: DataType, typeV3: Boolean = false,
    ytType: YtTypeHolder = YtTypeHolder.empty): Any = {
    value match {
      case uad: UnsafeArrayData =>
        val arrayData = uad.toArray[AnyRef](elementType).toSeq
        val elementYtType = ytType.getListItem
        arrayData.map(el => convertField(el, elementType, typeV3, elementYtType)).asJava
      case _ => value
    }
  }

  private def mapToYTreeListNode(value: Any, mapType: MapType, typeV3: Boolean,
    ytType: YtTypeHolder = YtTypeHolder.empty): YTreeListNode = {
    val map: Iterable[(Any, Any)] = getMapData(value, mapType.keyType, mapType.valueType)
    val convertedMap: Iterable[(Any, Any)] = if (typeV3) {
      convertMapTypeV3(map, mapType, ytType)
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
        // TBD: delete below UnsafeRow extends InternalRow
        // case row: UnsafeRow => convertField(row.get(index, field.dataType), field.dataType, typeV3, hint)
      }
    }

    val listBuilder = YTree.listBuilder
    convertedStruct.foreach(v => listBuilder.value(v))
    listBuilder.buildList
  }

  private def convertMapTypeV3(mapIterable: Iterable[(Any, Any)], mapType: MapType,
    ytType: YtTypeHolder): Iterable[(Any, Any)] = {
    if (!mapType.valueContainsNull) {
      mapIterable.foreach { case (_, value) =>
        if (value == null) {
          throw new IllegalArgumentException("Try to write null value to non-null column")
        }
      }
    }
    val keyYtType = ytType.getMapKey
    val valueYtType = ytType.getMapValue
    mapIterable.map { case (key, value) =>
      val convertedKey = convertField(key, mapType.keyType, typeV3 = true, keyYtType)
      val convertedValue = convertField(value, mapType.valueType, typeV3 = true, valueYtType)
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

object DynTableRowConverter {
  private[serializers] def isUnsignedInt(ytType: YtTypeHolder): Boolean = {
    ytType.ytType.exists(t => t.isUint8 || t.isUint16 || t.isUint32 || t.isUint64)
  }

  private[serializers] def isIntegerLikeType(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

  private lazy val sparkUInt64DataType: DataType = YTsaurusTypes.instance.sparkTypeFor(TiType.uint64())

  private[serializers] def isUInt64DataType(dataType: DataType): Boolean = dataType == sparkUInt64DataType

  private[serializers] def toUnsignedLong(value: Any): Long = value match {
    case v: Long => v
    case v: Int => java.lang.Integer.toUnsignedLong(v)
    case v: Short => java.lang.Short.toUnsignedInt(v).toLong
    case v: Byte => java.lang.Byte.toUnsignedInt(v).toLong
    case v: UInt64Long => v.toLong
    case v: java.lang.Number => v.longValue()
    case other => throw new IllegalArgumentException(
      s"Cannot convert ${other.getClass.getName} to unsigned long")
  }
}
