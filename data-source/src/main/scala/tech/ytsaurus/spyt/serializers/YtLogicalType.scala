package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.{TiType, TypeName}

import scala.annotation.tailrec

sealed trait SparkType {
  def topLevel: DataType
  def innerLevel: DataType
}
case class SingleSparkType(topLevel: DataType) extends SparkType {
  override def innerLevel: DataType = topLevel
}
case class TopInnerSparkTypes(topLevel: DataType, innerLevel: DataType) extends SparkType

sealed trait YtLogicalType {
  def value: Int = columnValueType.getValue

  def columnValueType: ColumnValueType

  def getNameV3(inner: Boolean): String = {
    if (inner) {
      alias.name
    } else {
      tiType.getTypeName.getWireName
    }
  }

  def getName(isColumnType: Boolean): String = {
    if (isColumnType) {
      columnValueType.getName
    } else {
      alias.name
    }
  }

  def tiType: TiType

  def sparkType: SparkType

  def nullable: Boolean = false

  def alias: YtLogicalTypeAlias

  def arrowSupported: Boolean = true
}

sealed trait YtLogicalTypeAlias {
  def name: String = aliases.head
  def aliases: Seq[String]
}

sealed abstract class AtomicYtLogicalType(name: String,
                                          override val value: Int,
                                          val columnValueType: ColumnValueType,
                                          val tiType: TiType,
                                          val sparkType: SparkType,
                                          otherAliases: Seq[String],
                                          override val arrowSupported: Boolean)
  extends YtLogicalType with YtLogicalTypeAlias {

  def this(name: String, value: Int, columnValueType: ColumnValueType, tiType: TiType, sparkType: DataType,
           otherAliases: Seq[String] = Seq.empty, arrowSupported: Boolean = true) =
    this(name, value, columnValueType, tiType, SingleSparkType(sparkType), otherAliases, arrowSupported)

  override def alias: YtLogicalTypeAlias = this
  override def aliases: Seq[String] = name +: otherAliases
}

sealed trait CompositeYtLogicalType extends YtLogicalType {
  override def columnValueType: ColumnValueType = ColumnValueType.ANY

  override def getName(isColumnType: Boolean): String = ColumnValueType.ANY.getName
}

sealed abstract class CompositeYtLogicalTypeAlias(name: String,
                                                  otherAliases: Seq[String] = Seq.empty) extends YtLogicalTypeAlias {
  override def aliases: Seq[String] = name +: otherAliases
}

object YtLogicalType {
  import tech.ytsaurus.spyt.types.YTsaurusTypes.instance.sparkTypeFor

  case object Null extends AtomicYtLogicalType("null", 0x02, ColumnValueType.NULL, TiType.nullType(), NullType)

  case object Int64 extends AtomicYtLogicalType("int64", 0x03, ColumnValueType.INT64, TiType.int64(), LongType)
  case object Uint64 extends AtomicYtLogicalType("uint64", 0x04, ColumnValueType.UINT64, TiType.uint64(), sparkTypeFor(TiType.uint64()))
  case object Float extends AtomicYtLogicalType("float", 0x05, ColumnValueType.DOUBLE, TiType.floatType(),
    TopInnerSparkTypes(FloatType, DoubleType), Seq.empty, arrowSupported = false)
  case object Double extends AtomicYtLogicalType("double", 0x05, ColumnValueType.DOUBLE, TiType.doubleType(), DoubleType)
  case object Boolean extends AtomicYtLogicalType("boolean", 0x06, ColumnValueType.BOOLEAN, TiType.bool(), BooleanType, Seq("bool"))

  case object String extends AtomicYtLogicalType("string", 0x10, ColumnValueType.STRING, TiType.string(), StringType)
  case object Binary extends AtomicYtLogicalType("binary", 0x10, ColumnValueType.STRING, TiType.string(), BinaryType) {
    override def getName(isColumnType: Boolean): String = columnValueType.getName

    override def getNameV3(inner: Boolean): String = {
      if (inner) alias.name else "string"
    }
  }
  case object Any extends AtomicYtLogicalType("any", 0x11, ColumnValueType.ANY, TiType.yson(), sparkTypeFor(TiType.yson()), Seq("yson")) {
    override def nullable: Boolean = true
  }

  case object Int8 extends AtomicYtLogicalType("int8", 0x1000, ColumnValueType.INT64, TiType.int8(), ByteType)
  case object Uint8 extends AtomicYtLogicalType("uint8", 0x1001, ColumnValueType.UINT64, TiType.uint8(), ShortType)

  case object Int16 extends AtomicYtLogicalType("int16", 0x1003, ColumnValueType.INT64, TiType.int16(), ShortType)
  case object Uint16 extends AtomicYtLogicalType("uint16", 0x1004, ColumnValueType.UINT64, TiType.uint16(), IntegerType)

  case object Int32 extends AtomicYtLogicalType("int32", 0x1005, ColumnValueType.INT64, TiType.int32(), IntegerType)
  case object Uint32 extends AtomicYtLogicalType("uint32", 0x1006, ColumnValueType.UINT64, TiType.uint32(), LongType)

  case object Utf8 extends AtomicYtLogicalType("utf8", 0x1007, ColumnValueType.STRING, TiType.utf8(), StringType)

  // Unsupported types are listed here: yt/yt/client/arrow/arrow_row_stream_encoder.cpp
  case object Date extends AtomicYtLogicalType("date", 0x1008, ColumnValueType.UINT64, TiType.date(), DateType, arrowSupported = false)
  case object Datetime extends AtomicYtLogicalType("datetime", 0x1009, ColumnValueType.UINT64, TiType.datetime(), new DatetimeType(), arrowSupported = false)
  case object Timestamp extends AtomicYtLogicalType("timestamp", 0x100a, ColumnValueType.UINT64, TiType.timestamp(), TimestampType, arrowSupported = false)
  case object Interval extends AtomicYtLogicalType("interval", 0x100b, ColumnValueType.INT64, TiType.interval(), LongType, arrowSupported = false)

  case object Void extends AtomicYtLogicalType("void", 0x100c, ColumnValueType.NULL, TiType.voidType(), NullType) //?

  case object Date32 extends AtomicYtLogicalType("date32", 0x1010, ColumnValueType.INT64, TiType.date32(), new Date32Type(), arrowSupported = false)
  case object Datetime64 extends AtomicYtLogicalType("datetime64", 0x1011, ColumnValueType.INT64, TiType.datetime64(), new Datetime64Type(), arrowSupported = false)
  case object Timestamp64 extends AtomicYtLogicalType("timestamp64", 0x1012, ColumnValueType.INT64, TiType.timestamp64(), new Timestamp64Type(), arrowSupported = false)
  case object Interval64 extends AtomicYtLogicalType("interval64", 0x1013, ColumnValueType.INT64, TiType.interval64(), new Interval64Type(), arrowSupported = false)

  case object Uuid extends AtomicYtLogicalType("uuid", 0x100f, ColumnValueType.STRING, TiType.uuid(), StringType, arrowSupported = false)
  case object Json extends AtomicYtLogicalType("json", 0x100e, ColumnValueType.STRING, TiType.json(), StringType, arrowSupported = false)

  case class Decimal(precision: Int, scale: Int) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(DecimalType(precision, scale))

    override def alias: CompositeYtLogicalTypeAlias = Decimal

    override def tiType: TiType = TiType.decimal(precision, scale)
  }

  case object Decimal extends CompositeYtLogicalTypeAlias("decimal")

  case class Optional(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def value: Int = inner.value

    override def columnValueType: ColumnValueType = inner.columnValueType

    override def tiType: TiType = TiType.optional(inner.tiType)

    override def sparkType: SparkType = inner.sparkType

    override def nullable: Boolean = true

    override def getName(isColumnType: Boolean): String = inner.getName(isColumnType)

    override def alias: CompositeYtLogicalTypeAlias = Optional

    override def arrowSupported: Boolean = inner.arrowSupported
  }

  case object Optional extends CompositeYtLogicalTypeAlias(TypeName.Optional.getWireName)

  private def resolveInnerType(sparkType: SparkType): DataType = sparkType match {
    case SingleSparkType(sType) => sType
    case TopInnerSparkTypes(_, innerLevel) => innerLevel
  }

  case class Dict(dictKey: YtLogicalType, dictValue: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(
      MapType(dictKey.sparkType.innerLevel, dictValue.sparkType.innerLevel, dictValue.nullable)
    )

    override def tiType: TiType = TiType.dict(dictKey.tiType, dictValue.tiType)

    override def alias: CompositeYtLogicalTypeAlias = Dict
  }

  case object Dict extends CompositeYtLogicalTypeAlias(TypeName.Dict.getWireName)

  case class Array(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType( ArrayType(inner.sparkType.innerLevel, inner.nullable))

    override def tiType: TiType = TiType.list(inner.tiType)

    override def alias: CompositeYtLogicalTypeAlias = Array
  }

  case object Array extends CompositeYtLogicalTypeAlias(TypeName.List.getWireName)

  case class Struct(fields: Seq[(String, YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(StructType(fields
      .map { case (name, ytType, meta) => getStructField(name, ytType, meta, topLevel = false) }))

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.struct(
      fields.map{ case (name, ytType, _) => new Member(name, ytType.tiType)}.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Struct
  }

  case object Struct extends CompositeYtLogicalTypeAlias(TypeName.Struct.getWireName)

  case class Tuple(elements: Seq[(YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(StructType(elements.zipWithIndex
      .map { case ((ytType, meta), index) => getStructField(s"_${1 + index}", ytType, meta, topLevel = false) }))

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.tuple(
      elements.map { case (e, _) => e.tiType } .asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Tuple
  }

  case object Tuple extends CompositeYtLogicalTypeAlias(TypeName.Tuple.getWireName)

  case class Tagged(inner: YtLogicalType, tag: String) extends CompositeYtLogicalType {
    override def sparkType: SparkType = inner.sparkType

    override def tiType: TiType = TiType.tagged(inner.tiType, tag)

    override def alias: CompositeYtLogicalTypeAlias = Tagged
  }

  case object Tagged extends CompositeYtLogicalTypeAlias(TypeName.Tagged.getWireName)

  case class VariantOverStruct(fields: Seq[(String, YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(StructType(fields.map { case (name, ytType, meta) =>
      getStructField(s"_v$name", ytType, meta, forcedNullability = Some(true), topLevel = false) }))

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.variantOverStruct(
      fields.map{ case (name, ytType, _) => new Member(name, ytType.tiType)}.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Variant
  }

  case class VariantOverTuple(fields: Seq[(YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(
      StructType(fields.zipWithIndex.map { case ((ytType, meta), index) =>
        getStructField(s"_v_${1 + index}", ytType, meta, forcedNullability = Some(true), topLevel = false) })
    )

    import scala.collection.JavaConverters._
    override def tiType: TiType = TiType.variantOverTuple(
      fields.map { case (e, _) => e.tiType }.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Variant
  }

  case object Variant extends CompositeYtLogicalTypeAlias(TypeName.Variant.getWireName)

  private lazy val atomicTypes = Seq(Null, Int64, Uint64, Float, Double, Boolean, String, Binary, Any,
    Int8, Uint8, Int16, Uint16, Int32, Uint32, Utf8, Date, Datetime, Timestamp, Interval, Date32, Datetime64,
    Timestamp64, Interval64, Json, Uuid, Void)

  private lazy val compositeTypes = Seq(Optional, Dict, Array, Struct, Tuple,
    Tagged, Variant, Decimal)

  def fromName(name: String): YtLogicalType = {
    findOrThrow(name, atomicTypes)
  }

  def fromCompositeName(name: String): YtLogicalTypeAlias = {
    findOrThrow(name, compositeTypes)
  }

  private def findOrThrow[T <: YtLogicalTypeAlias](name: String, types: Seq[T]): T = {
    types.find(_.aliases.contains(name))
      .getOrElse(throw new IllegalArgumentException(s"Unknown logical yt type: $name"))
  }

  def getStructField(name: String, ytType: YtLogicalType, metadata: Metadata = Metadata.empty,
                     forcedNullability: Option[Boolean] = None, topLevel: Boolean = true): StructField = {
    val metadataBuilder = new MetadataBuilder
    metadataBuilder.withMetadata(metadata)
    addInnerMetadata(metadataBuilder, ytType)
    forcedNullability.foreach(_ => metadataBuilder.putBoolean(MetadataFields.OPTIONAL, ytType.nullable))
    StructField(
      name,
      if (topLevel) ytType.sparkType.topLevel else ytType.sparkType.innerLevel,
      forcedNullability.getOrElse(ytType.nullable),
      metadataBuilder.build()
    )
  }

  @tailrec
  private def addInnerMetadata(metadataBuilder: MetadataBuilder, ytType: YtLogicalType): Unit = {
    ytType match {
      case o: Optional => addInnerMetadata(metadataBuilder, o.inner)
      case t: Tagged => metadataBuilder.putString(MetadataFields.TAG, t.tag)
      case YtLogicalType.Uuid | YtLogicalType.Json =>
        metadataBuilder.putString(MetadataFields.YT_LOGICAL_TYPE, ytType.getNameV3(true))
      case _ =>
    }
  }
}
