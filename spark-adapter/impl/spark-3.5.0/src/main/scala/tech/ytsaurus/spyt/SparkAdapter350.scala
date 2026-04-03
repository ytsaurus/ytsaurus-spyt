package tech.ytsaurus.spyt

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.types.{DataTypeUtils, PhysicalNumericType}
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.types.{NumericType, StructType}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase
import tech.ytsaurus.spyt.format.{YtPartitionedFile350, YtPartitioningDelegate}

trait SparkAdapter350 extends SparkAdapter {
  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
    start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, SparkPath.fromUrlString(filePath), start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile350[T](delegate)
  }

  override def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row] = ExpressionEncoder(schema)

  override def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    DataTypeUtils.toAttributes(schema)
  }

  override def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus] = pd.files.map(_.fileStatus)

  override def castToLong(x: NumericType): Any => Any = {
    val numeric = PhysicalNumericType.numeric(x)
    b => numeric.toLong(b)
  }

  override def fromAttributes(attributes: Seq[Attribute]): StructType = {
    DataTypeUtils.fromAttributes(attributes)
  }
}
