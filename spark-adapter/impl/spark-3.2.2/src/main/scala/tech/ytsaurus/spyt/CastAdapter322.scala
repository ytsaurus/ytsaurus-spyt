package tech.ytsaurus.spyt
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.types.DataType

@MinSparkVersion("3.2.2")
class CastAdapter322 extends CastAdapter {
  override def createCast(expr: Expression, dataType: DataType): Cast = Cast(expr, dataType)
}
