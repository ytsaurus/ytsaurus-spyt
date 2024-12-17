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
import scala.collection.JavaConverters._

trait SparkAdapter
  extends SparkAdapterBase
  with PartitionedFileAdapter
  with PartitionedFilePathAdapter
  with YtPartitionReaderFactoryCreator
  with ShuffleAdapter
  with ResourcesAdapter
  with EncoderAdapter
  with SchemaAdapter
  with PartitionDirectoryAdapter
  with DataTypeAdapter
  with DataSourceV2ScanRelationAdapter
  with CastAdapter

trait SparkAdapterBase {
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
}

trait PartitionedFileAdapter {
  def createPartitionedFile(partitionValues: InternalRow, filePath: String, start: Long, length: Long): PartitionedFile
  def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T]
}

trait PartitionedFilePathAdapter {
  def getStringFilePath(pf: PartitionedFile): String
  def getHadoopFilePath(pf: PartitionedFile): Path
}

trait YtPartitionReaderFactoryCreator {
  def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory
}

trait ShuffleAdapter {
  def createShufflePartitioner(numPartitions: Int): Partitioner
}

trait ResourcesAdapter {
  // Here we use Product instead of ResourceProfile.ExecutorResourcesOrDefaults class because this class
  // has private visibility. We must explicitly cast the argument to this class in implementations that should
  // reside in org.apache.spark.resource package
  def getExecutorCores(execResources: Product): Int
}

trait EncoderAdapter {
  def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row]
}

trait SchemaAdapter {
  def schemaToAttributes(schema: StructType): Seq[AttributeReference]
}

trait PartitionDirectoryAdapter {
  def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus]
}

trait DataTypeAdapter {
  def castToLong(x: NumericType): Any => Any
  def fromAttributes(attributes: Seq[Attribute]): StructType
}

trait DataSourceV2ScanRelationAdapter {
  def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation
}

trait CastAdapter {
  def createCast(expr: Expression, dataType: DataType): Cast
}

private class SparkAdapterImpl(base: SparkAdapterBase,
                               pfAdapter: PartitionedFileAdapter,
                               pfPathAdapter: PartitionedFilePathAdapter,
                               ytprfc: YtPartitionReaderFactoryCreator,
                               shuffleAdapter: ShuffleAdapter,
                               resourcesAdapter: ResourcesAdapter,
                               encoderAdapter: EncoderAdapter,
                               schemaAdapter: SchemaAdapter,
                               pdAdapter: PartitionDirectoryAdapter,
                               dataTypeAdapter: DataTypeAdapter,
                               dsv2Adapter: DataSourceV2ScanRelationAdapter,
                               castAdapter: CastAdapter
                              ) extends SparkAdapter {

  override def createYtScanOutputPartitioning(numPartitions: Int): Partitioning =
    base.createYtScanOutputPartitioning(numPartitions)

  override def createYtScanBuilder(scanBuilderAdapter: ScanBuilderAdapter): FileScanBuilder =
    base.createYtScanBuilder(scanBuilderAdapter)

  override def executorBackendFactory: ExecutorBackendFactory =
    base.executorBackendFactory

  override def parserUtilsWithOrigin[T](ctx: ParserRuleContext)(f: => T): T =
    base.parserUtilsWithOrigin(ctx)(f)

  override def checkPushedFilters(scanBuilder: ScanBuilder, filters: Seq[Expression],
                                  expected: Seq[Filter]): (Seq[Any], Seq[Any]) =
    base.checkPushedFilters(scanBuilder, filters, expected)

  override def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition =
    base.getInputPartition(dsrddPartition)

  override def createLoggingEvent(fqnOfCategoryClass: String, logger: Category, timeStamp: Long, level: Level,
                                  message: String, throwable: Throwable): LoggingEvent =
    base.createLoggingEvent(fqnOfCategoryClass, logger, timeStamp, level, message, throwable)

  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
                                     start: Long, length: Long): PartitionedFile =
    pfAdapter.createPartitionedFile(partitionValues, filePath, start, length)

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] =
    pfAdapter.createYtPartitionedFile(delegate)

  override def getStringFilePath(pf: PartitionedFile): String =
    pfPathAdapter.getStringFilePath(pf)

  override def getHadoopFilePath(pf: PartitionedFile): Path =
    pfPathAdapter.getHadoopFilePath(pf)

  override def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory =
    ytprfc.createYtPartitionReaderFactory(adapter)

  override def createShufflePartitioner(numPartitions: Int): Partitioner =
    shuffleAdapter.createShufflePartitioner(numPartitions)

  override def getExecutorCores(execResources: Product): Int =
    resourcesAdapter.getExecutorCores(execResources)

  override def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row] =
    encoderAdapter.createExpressionEncoder(schema)

  override def schemaToAttributes(schema: StructType): Seq[AttributeReference] =
    schemaAdapter.schemaToAttributes(schema)

  override def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus] =
    pdAdapter.getPartitionFileStatuses(pd)

  override def castToLong(x: NumericType): Any => Any =
    dataTypeAdapter.castToLong(x)

  override def fromAttributes(attributes: Seq[Attribute]): StructType =
    dataTypeAdapter.fromAttributes(attributes)

  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation =
    dsv2Adapter.copyDataSourceV2ScanRelation(rel, newScan)

  override def createCast(expr: Expression, dataType: DataType): Cast =
    castAdapter.createCast(expr, dataType)
}

object SparkAdapter {

  private val log = LoggerFactory.getLogger(getClass)

  lazy val instance: SparkAdapter = createInstance()

  private def createInstance(): SparkAdapter = {
    val componentInterfaces = classOf[SparkAdapter].getInterfaces
    val components = componentInterfaces.map(getAdapterComponent)
    val implConstructor = classOf[SparkAdapterImpl].getConstructor(componentInterfaces: _*)
    implConstructor.newInstance(components: _*)
  }

  private def getAdapterComponent(componentClass: Class[_]): AnyRef = {
    val instances = ServiceLoader.load(componentClass).asScala
    log.debug(s"Num of available ${componentClass.getName} instances: ${instances.size}")
    val (minSparkVersion, instance) = instances.map { instance =>
      val minSparkVersion = instance.getClass.getAnnotation(classOf[MinSparkVersion]).value()
      (minSparkVersion, instance)
    }.filter { case (minSparkVersion, _) =>
      SparkVersionUtils.ordering.lteq(minSparkVersion, SparkVersionUtils.currentVersion)
    }.toList.sortBy(_._1)(SparkVersionUtils.ordering.reverse).head

    log.debug(s"Runtime Spark version: ${SparkVersionUtils.currentVersion}," +
      s" using ${componentClass.getName} for $minSparkVersion")

    instance.asInstanceOf[AnyRef]
  }
}
