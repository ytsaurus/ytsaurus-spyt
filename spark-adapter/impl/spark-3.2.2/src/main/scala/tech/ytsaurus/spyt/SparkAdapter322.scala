package tech.ytsaurus.spyt

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.log4j.{Category, Level}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.Partitioner
import org.apache.spark.executor.{ExecutorBackendFactory, ExecutorBackendFactory322}
import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults
import org.apache.spark.sql.{AdapterSupport322, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression}
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.ShuffleSupport
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanRelation, FileScanBuilder, PushDownUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, NumericType, StructType}
import org.apache.spark.sql.v2.{PartitionReaderFactoryAdapter, ScanBuilderAdapter, YtPartitionReaderFactory322, YtScanBuilder322, YtScanPartitioning}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase
import tech.ytsaurus.spyt.format.{YtPartitionedFile322, YtPartitioningDelegate}

trait SparkAdapter322 extends SparkAdapter {
  override def createYtScanOutputPartitioning(nPartitions: Int): Partitioning = YtScanPartitioning(nPartitions)

  override def createYtScanBuilder(scanBuilderAdapter: ScanBuilderAdapter): FileScanBuilder = {
    new YtScanBuilder322(scanBuilderAdapter)
  }

  override def executorBackendFactory: ExecutorBackendFactory = ExecutorBackendFactory322

  override def parserUtilsWithOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    ParserUtils.withOrigin(ctx)(f)
  }

  override def checkPushedFilters(scanBuilder: ScanBuilder,
    filters: Seq[Expression],
    expected: Seq[Filter]): (Seq[Any], Seq[Any]) = {
    (PushDownUtils.pushFilters(scanBuilder, filters)._1, expected)
  }

  override def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition = {
    dsrddPartition.inputPartition
  }

  override def createLoggingEvent(fqnOfCategoryClass: String, logger: Category,
    timeStamp: Long, level: Level,
    message: String, throwable: Throwable): LoggingEvent = {
    new LoggingEvent(fqnOfCategoryClass, logger, timeStamp, level, message, throwable)
  }

  override def defaultModuleOptions(): String = ""

  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
    start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, filePath, start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile322(delegate)
  }

  override def getStringFilePath(pf: PartitionedFile): String = pf.filePath

  override def getHadoopFilePath(pf: PartitionedFile): Path = new Path(pf.filePath)

  override def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory =
    YtPartitionReaderFactory322(adapter)

  override def createShufflePartitioner(numPartitions: Int): Partitioner =
    ShuffleSupport.createShufflePartitioner(numPartitions)

  override def getExecutorCores(execResources: Product): Int = AdapterSupport322.getExecutorCores(execResources)

  override def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder(schema)
  }

  override def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    AdapterSupport322.schemaToAttributes(schema)
  }

  override def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus] = pd.files

  override def castToLong(x: NumericType): Any => Any = AdapterSupport322.castToLong(x)

  override def fromAttributes(attributes: Seq[Attribute]): StructType = AdapterSupport322.fromAttributes(attributes)

  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation = {
    rel.copy(scan = newScan)
  }

  override def createCast(expr: Expression, dataType: DataType): Cast = Cast(expr, dataType)
}
