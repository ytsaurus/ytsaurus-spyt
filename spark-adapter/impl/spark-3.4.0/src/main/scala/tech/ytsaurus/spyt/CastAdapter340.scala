package tech.ytsaurus.spyt

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.types.DataType

@MinSparkVersion("3.4.0")
class CastAdapter340 extends CastAdapter {
  override def createCast(expr: Expression, dataType: DataType): Cast = Cast(expr, dataType)
}
