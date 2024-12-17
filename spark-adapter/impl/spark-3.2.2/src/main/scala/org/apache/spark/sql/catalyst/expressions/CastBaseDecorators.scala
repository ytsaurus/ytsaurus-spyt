package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, ExprValue}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, NullType}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.CastBase")
@Applicability(to = "3.3.4")
class CastBaseDecorators {

  @DecoratedMethod
  protected[this] def cast(from: DataType, to: DataType): Any => Any = {
    if (DataType.equalsStructurally(from, to)) {
      __cast(from, to)
    } else if (from == NullType) {
      YTsaurusCastUtils.cannotCastFromNullTypeError(to)
    } else {
      to match {
        case ts.uInt64DataType => ts.uInt64Cast(from)
        case ts.ysonDataType => ts.ysonCast(from)
        case _ => __cast(from, to)
      }
    }
  }

  protected[this] def __cast(from: DataType, to: DataType): Any => Any = ???

  protected[this] type CastFunction = (ExprValue, ExprValue, ExprValue) => Block

  @DecoratedMethod
  private[this] def castToString(from: DataType): Any => Any = from match {
    case ts.uInt64DataType => ts.uInt64CastToString
    case _ => __castToString(from)
  }

  private[this] def __castToString(from: DataType): Any => Any = ???

  @DecoratedMethod
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case ts.ysonDataType => ts.ysonCastToBinary
    case _ => __castToBinary(from)
  }

  private[this] def __castToBinary(from: DataType): Any => Any = ???


  @DecoratedMethod
  private[this] def castToTimestamp(from: DataType): Any => Any = {
    from match {
      case dt if ts.isDateTimeDataType(dt) => ts.dateTimeCastToTimestamp
      case _ => __castToTimestamp(from)
    }
  }

  private[this] def __castToTimestamp(from: DataType): Any => Any = ???


  @DecoratedMethod
  private[this] def nullSafeCastFunction(from: DataType, to: DataType, ctx: CodegenContext): CastFunction = to match {
    case ts.ysonDataType if !(from == NullType || to == from) => ts.binaryCastToYsonCode
    case _ => __nullSafeCastFunction(from, to, ctx)
  }

  private[this] def __nullSafeCastFunction(from: DataType, to: DataType, ctx: CodegenContext): CastFunction = ???

  @DecoratedMethod
  @Applicability(to = "3.4.4")
  private[this] def castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case ts.uInt64DataType => ts.uInt64CastToStringCode
    case _ => __castToStringCode(from, ctx)
  }

  private[this] def __castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = ???

  @DecoratedMethod
  private[this] def castToBinaryCode(from: DataType): CastFunction = from match {
    case ts.ysonDataType => ts.ysonCastToBinaryCode
    case _ => __castToBinaryCode(from)
  }

  private[this] def __castToBinaryCode(from: DataType): CastFunction = ???

  @DecoratedMethod
  private[this] def castToTimestampCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case dt if ts.isDateTimeDataType(dt) => ts.dateTimeCastToTimestampCode
    case _ => __castToTimestampCode(from, ctx)
  }

  private[this] def __castToTimestampCode(from: DataType, ctx: CodegenContext): CastFunction = ???
}

object YTsaurusCastUtils {
  def cannotCastFromNullTypeError(to: DataType): Any => Any = {
    _ => throw QueryExecutionErrors.cannotCastFromNullTypeError(to)
  }
}
