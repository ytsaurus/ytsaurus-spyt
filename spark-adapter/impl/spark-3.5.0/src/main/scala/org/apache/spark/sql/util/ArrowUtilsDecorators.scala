package org.apache.spark.sql.util

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.SparkEnv
import org.apache.spark.adapter.Config.YTSAURUS_ARROW_STRING_TO_BINARY
import org.apache.spark.sql.types.{DataType, StringType, StructType}

import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{AddMethod, Applicability, Decorate, DecoratedMethod, OriginClass}

import java.util.{ArrayList, HashMap}

@Decorate
@OriginClass("org.apache.spark.sql.util.ArrowUtils$")
@Applicability(from = "3.5.0")
private[sql] object ArrowUtilsDecorators {

  @DecoratedMethod
  @Applicability(to = "4.3.0")
  def toArrowSchema(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean): Schema = {
    val arrowSchema = __toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
    ArrowYsonSchema.addMetadata(schema, arrowSchema)
  }

  def __toArrowSchema(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean): Schema = ???

  @DecoratedMethod
  def toArrowType(dt: DataType, timeZoneId: String, largeVarTypes: Boolean): ArrowType = {
    val stringToBinary: Boolean = SparkEnv.get.conf.get(YTSAURUS_ARROW_STRING_TO_BINARY)
    dt match {
      case ts.uInt64DataType => new ArrowType.Int(8 * 8, false)
      case ts.ysonDataType if !largeVarTypes => ArrowType.Binary.INSTANCE
      case ts.ysonDataType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
      case StringType if stringToBinary && !largeVarTypes => ArrowType.Binary.INSTANCE
      case StringType if stringToBinary && largeVarTypes => ArrowType.LargeBinary.INSTANCE
      case _ => __toArrowType(dt, timeZoneId, largeVarTypes)
    }
  }

  def __toArrowType(dt: DataType, timeZoneId: String, largeVarTypes: Boolean): ArrowType = ???

  @DecoratedMethod
  def fromArrowType(dt: ArrowType): DataType = dt match {
    case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 8 => ts.uInt64DataType
    case _ => __fromArrowType(dt)
  }

  def __fromArrowType(dt: ArrowType): DataType = ???

  @AddMethod
  @Applicability(to = "4.3.0")
  def toArrowSchema$default$5(): Boolean = false

  @AddMethod
  @Applicability(to = "4.3.0")
  def toArrowSchema(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean,
    losslessInternalTypes: Boolean): Schema = {
    toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
  }

  @DecoratedMethod(name = "toArrowSchema")
  @Applicability(from = "4.3.0")
  def toArrowSchema430(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean,
    losslessInternalTypes: Boolean): Schema = {
    val arrowSchema = __toArrowSchema(
      schema,
      timeZoneId,
      errorOnDuplicatedFieldNames,
      largeVarTypes,
      losslessInternalTypes)
    ArrowYsonSchema.addMetadata(schema, arrowSchema)
  }

  def __toArrowSchema(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean,
    losslessInternalTypes: Boolean): Schema = ???
}

private[sql] object ArrowYsonSchema {

  def addMetadata(schema: StructType, arrowSchema: Schema): Schema = {
    // Spark unwraps UDTs before toArrowField, so use the original schema to identify YSON columns.
    val fields = new ArrayList[Field](arrowSchema.getFields)
    for (index <- 0 until schema.fields.length) {
      if (schema.fields(index).dataType == ts.ysonDataType) {
        val field = fields.get(index)
        val metadata = new HashMap[String, String](field.getMetadata)
        metadata.put("YtType", "yson")
        val fieldType = new FieldType(field.isNullable, field.getType, field.getDictionary, metadata)
        fields.set(index, new Field(field.getName, fieldType, field.getChildren))
      }
    }
    new Schema(fields, arrowSchema.getCustomMetadata)
  }
}
