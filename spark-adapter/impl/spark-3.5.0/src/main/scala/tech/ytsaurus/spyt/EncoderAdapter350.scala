package tech.ytsaurus.spyt
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

@MinSparkVersion("3.5.0")
class EncoderAdapter350 extends EncoderAdapter {
  override def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row] = ExpressionEncoder(schema)
}
