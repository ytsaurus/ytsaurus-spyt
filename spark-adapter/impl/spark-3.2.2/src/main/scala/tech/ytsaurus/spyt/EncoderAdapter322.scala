package tech.ytsaurus.spyt
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

@MinSparkVersion("3.2.2")
class EncoderAdapter322 extends EncoderAdapter {

  override def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder(schema)
  }
}
