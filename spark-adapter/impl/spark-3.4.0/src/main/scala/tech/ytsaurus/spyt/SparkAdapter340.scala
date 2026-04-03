package tech.ytsaurus.spyt

import org.apache.hadoop.fs.Path
import org.apache.spark.{Partitioner, ShuffleSupport}
import org.apache.spark.paths.SparkPath
import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults
import org.apache.spark.sql.AdapterSupport340
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.v2.{PartitionReaderFactoryAdapter, YtPartitionReaderFactory340}
import tech.ytsaurus.spyt.format.{YtPartitionedFile340, YtPartitioningDelegate}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase

trait SparkAdapter340 extends SparkAdapter {
  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
    start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, SparkPath.fromUrlString(filePath), start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile340[T](delegate)
  }

  override def getStringFilePath(pf: PartitionedFile): String = pf.filePath.urlEncoded

  override def getHadoopFilePath(pf: PartitionedFile): Path = pf.filePath.toPath

  override def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory = {
    YtPartitionReaderFactory340(adapter)
  }

  override def createShufflePartitioner(numPartitions: Int): Partitioner =
    ShuffleSupport.createShufflePartitioner(numPartitions)

  override def getExecutorCores(execResources: Product): Int = AdapterSupport340.getExecutorCores(execResources)

  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation = {
    rel.copy(scan = newScan)
  }

  override def createCast(expr: Expression, dataType: DataType): Cast = Cast(expr, dataType)
}
