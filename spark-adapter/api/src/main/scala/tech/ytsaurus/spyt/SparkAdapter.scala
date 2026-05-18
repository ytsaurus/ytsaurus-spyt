package tech.ytsaurus.spyt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.deploy.history.{YtLogPage, YtLogPageDelegate}
import org.apache.spark.deploy.rest.RestServletCompat
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Row, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.PrimitiveDataTypeContext
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanExecDelegate}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, NumericType, StructType}
import org.apache.spark.sql.v2.PartitionReaderFactoryAdapter
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.collection.BitSet
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.YtPartitioningDelegate
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase

import java.io.File
import java.net.URI
import java.util.ServiceLoader
import scala.reflect.ClassTag

trait SparkAdapter {
  def pushFilters(scanBuilder: ScanBuilder, filters: Seq[Expression]): Either[Seq[Filter], Seq[Predicate]]
  def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition

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

  def dfShowString(df: Dataset[_], numRows: Int, truncate: Int): String

  def createAnalysisException(message: String): AnalysisException

  def filterRootPaths(rootPaths: Seq[Path], hadoopConf: Configuration): Seq[Path]

  def getSQLConf(sqlContext: SQLContext): SQLConf

  def createStreamingDataFrame(sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame

  def sparkImplicits(spark: SparkSession): SQLImplicits

  def createCatalogStorageFormat(locationUri: Option[URI]): CatalogStorageFormat

  def createFileSourceScanExecDelegate(
    relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    supportsColumnarProvider: HadoopFsRelation => Boolean): FileSourceScanExec with FileSourceScanExecDelegate

  def createColumn(source: Seq[Column], constructor: Seq[Expression] => Expression): Column

  def applySchemaToPythonRDD(spark: SparkSession, rdd: RDD[Array[Any]], schemaString: String): DataFrame

  def createStaticInvoke(clazz: Class[_], returnType: DataType, methodName: String, args: Seq[Expression]): Expression

  def copyDataSourceV2Relation(rel: DataSourceV2Relation, table: Table): DataSourceV2Relation

  def isUint64DataTypeContext(ctx: PrimitiveDataTypeContext, identifierId: Int): Boolean = {
    throw new UnsupportedOperationException("isUint64DataTypeContext is not supported")
  }

  def parseDataTypeJson(json: String): DataType

  def mapPartitionsWithIndexInternal[T, U: ClassTag](
    rdd: RDD[T],
    f: (Int, Iterator[T]) => Iterator[U],
    isOrderSensitive: Boolean): RDD[U]

  def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C]

  def createCatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String]): CatalogTable

  def offsetAsJson(offset: Offset): String

  def copyAggregate(agg: Aggregate, child: LogicalPlan): Aggregate

  def driverMemoryOverheadMinMib(conf: SparkConf): Long

  // The result must be cast to ResourceProfile.ExecutorResourcesOrDefaults because it is visible
  // only in org.apache.spark package
  def getResourcesForClusterManager(
    resourceProfile: ResourceProfile,
    memoryOverheadFactor: Double,
    conf: SparkConf,
    isPythonApp: Boolean): AnyRef

  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String]

  // The result must be cast to RestServlet or its subclasses because it is visible only
  // in org.apache.spark.deploy.rest package
  def wrapRestServlet(servlet: RestServletCompat): AnyRef

  def getHistoryProviderName(conf: SparkConf): String

  def createYtLogPage(delegate: YtLogPageDelegate): YtLogPage

  // The result must be cast to JettyUtils.ServletParams[String] because it is visible
  // only in org.apache.spark package
  def createServletParams(ytLogPageDelegate: YtLogPageDelegate): AnyRef

  def createColumnarBatchRowWrapper(
    row: InternalRow,
    getStructImpl: (Int, Int) => InternalRow,
    getArrayImpl: Int => ArrayData,
    getMapImpl: Int => MapData): InternalRow

  def getFileSourceScanExecStream(scan: FileSourceScanExec): Option[SparkDataStream]

  def getHadoopConf(sparkConf: SparkConf): Configuration

  def fetchFile(url: String, targetDir: File, conf: SparkConf): File
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
