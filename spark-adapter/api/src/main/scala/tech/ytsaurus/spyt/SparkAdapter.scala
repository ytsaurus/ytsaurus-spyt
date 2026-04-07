package tech.ytsaurus.spyt

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.log4j.{Category, Level}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.Partitioner
import org.apache.spark.executor.ExecutorBackendFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanRelation, FileScanBuilder}
import org.apache.spark.sql.{Row, sources}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, NumericType, StructType}
import org.apache.spark.sql.v2.{PartitionReaderFactoryAdapter, ScanBuilderAdapter}
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.YtPartitioningDelegate
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

trait SparkAdapter {
  def createYtScanOutputPartitioning(numPartitions: Int): Partitioning
  def createYtScanBuilder(scanBuilderAdapter: ScanBuilderAdapter): FileScanBuilder
  def executorBackendFactory: ExecutorBackendFactory
  def parserUtilsWithOrigin[T](ctx: ParserRuleContext)(f: => T): T

  //These methods are used for backward compatibility with Spark 3.2.2
  def checkPushedFilters(scanBuilder: ScanBuilder,
    filters: Seq[Expression],
    expected: Seq[sources.Filter]): (Seq[Any], Seq[Any])
  def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition
  def createLoggingEvent(fqnOfCategoryClass: String, logger: Category,
    timeStamp: Long, level: Level, message: String,
    throwable: Throwable): LoggingEvent
  def defaultModuleOptions(): String

  def createPartitionedFile(partitionValues: InternalRow, filePath: String, start: Long, length: Long): PartitionedFile
  def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T]

  def getStringFilePath(pf: PartitionedFile): String
  def getHadoopFilePath(pf: PartitionedFile): Path

  def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory

  def createShufflePartitioner(numPartitions: Int): Partitioner

  // Here we use Product instead of ResourceProfile.ExecutorResourcesOrDefaults class because this class
  // has private visibility. We must explicitly cast the argument to this class in implementations that should
  // reside in org.apache.spark.resource package
  def getExecutorCores(execResources: Product): Int

  def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row]

  def schemaToAttributes(schema: StructType): Seq[AttributeReference]

  def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus]

  def castToLong(x: NumericType): Any => Any
  def fromAttributes(attributes: Seq[Attribute]): StructType

  def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation

  def createCast(expr: Expression, dataType: DataType): Cast
}

trait SparkAdapterProvider {
  def createSparkAdapter(sparkVersion: String): SparkAdapter
}


object SparkAdapter {

  private val log = LoggerFactory.getLogger(getClass)

  lazy val instance: SparkAdapter = createInstance()

  private def createInstance(): SparkAdapter = {
    val providerLoader = ServiceLoader.load(classOf[SparkAdapterProvider])
    val provider = providerLoader.findFirst().orElseThrow()
    val adapter = provider.createSparkAdapter(SparkVersionUtils.currentVersion)
    log.debug(s"Runtime Spark version: ${SparkVersionUtils.currentVersion}, using ${adapter.getClass} adapter")
    adapter
  }
}
