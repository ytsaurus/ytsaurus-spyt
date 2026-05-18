package tech.ytsaurus.spyt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.deploy.history.{YtLogPage, YtLogPageDelegate}
import org.apache.spark.deploy.rest.{RestServletCompat, RestServletSupport330}
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.{AdapterSupport330, AnalysisException, Column, DataFrame, Dataset, Row, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2Relation, DataSourceV2ScanRelation, PushDownUtils}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.{FileStreamSink, SerializedOffset}
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanExecDelegate, ShuffleSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, NumericType, StructType}
import org.apache.spark.sql.v2.{PartitionReaderFactoryAdapter, YtPartitionReaderFactory330}
import org.apache.spark.sql.vectorized.ColumnarBatchRowWrapperBase
import org.apache.spark.util.collection.BitSet
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase
import tech.ytsaurus.spyt.format.{YtPartitionedFile330, YtPartitioningDelegate}

import java.io.File
import java.net.URI
import scala.reflect.ClassTag

trait SparkAdapter330 extends SparkAdapter {

  override def pushFilters(scanBuilder: ScanBuilder, filters: Seq[Expression]): Either[Seq[Filter], Seq[Predicate]] = {
    PushDownUtils.pushFilters(scanBuilder, filters)._1
  }

  override def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition = {
    dsrddPartition.inputPartitions.head
  }

  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
    start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, filePath, start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile330(delegate)
  }

  override def getStringFilePath(pf: PartitionedFile): String = pf.filePath

  override def getHadoopFilePath(pf: PartitionedFile): Path = new Path(pf.filePath)

  override def createYtPartitionReaderFactory(adapter: PartitionReaderFactoryAdapter): PartitionReaderFactory =
    YtPartitionReaderFactory330(adapter)

  override def createShufflePartitioner(numPartitions: Int): Partitioner =
    ShuffleSupport.createShufflePartitioner(numPartitions)

  override def getExecutorCores(execResources: Product): Int = AdapterSupport330.getExecutorCores(execResources)

  override def createExpressionEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder(schema)
  }

  override def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    AdapterSupport330.schemaToAttributes(schema)
  }

  override def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus] = pd.files

  override def castToLong(x: NumericType): Any => Any = AdapterSupport330.castToLong(x)

  override def fromAttributes(attributes: Seq[Attribute]): StructType = AdapterSupport330.fromAttributes(attributes)

  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation = {
    rel.copy(scan = newScan)
  }

  override def createCast(expr: Expression, dataType: DataType): Cast = Cast(expr, dataType)

  override def dfShowString(df: Dataset[_], numRows: Int, truncate: Int): String = {
    AdapterSupport330.dfShowString(df, numRows, truncate)
  }

  override def createAnalysisException(message: String): AnalysisException = {
    AdapterSupport330.createAnalysisException(message)
  }

  override def filterRootPaths(rootPathsSpecified: Seq[Path], hadoopConf: Configuration): Seq[Path] = {
    rootPathsSpecified.filterNot(FileStreamSink.ancestorIsMetadataDirectory(_, hadoopConf))
  }

  override def getSQLConf(sqlContext: SQLContext): SQLConf = AdapterSupport330.getSQLConf(sqlContext)

  override def createStreamingDataFrame(
    sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
    AdapterSupport330.createStreamingDataFrame(sqlContext, rdd, schema)
  }

  override def sparkImplicits(spark: SparkSession): SQLImplicits = AdapterSupport330.sparkImplicits(spark)

  override def createCatalogStorageFormat(locationUri: Option[URI]): CatalogStorageFormat = {
    CatalogStorageFormat(
      locationUri = locationUri,
      inputFormat = None, outputFormat = None, serde = None, compressed = false, properties = Map.empty
    )
  }

  override def createFileSourceScanExecDelegate(
    relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    supportsColumnarProvider: HadoopFsRelation => Boolean): FileSourceScanExec with FileSourceScanExecDelegate = {

    new FileSourceScanExec(relation, output, requiredSchema, partitionFilters, optionalBucketSet,
      None, dataFilters, tableIdentifier) with FileSourceScanExecDelegate {

      override lazy val supportsColumnar: Boolean = supportsColumnarProvider(relation)
    }
  }

  override def createColumn(source: Seq[Column], constructor: Seq[Expression] => Expression): Column = {
    new Column(constructor(source.map(_.expr)))
  }

  override def applySchemaToPythonRDD(spark: SparkSession, rdd: RDD[Array[Any]], schemaString: String): DataFrame = {
    AdapterSupport330.applySchemaToPythonRDD(spark, rdd, schemaString)
  }

  override def createStaticInvoke(
    clazz: Class[_], returnType: DataType, methodName: String, args: Seq[Expression]): Expression = {
    StaticInvoke(clazz, returnType, methodName, args, returnNullable = false)
  }

  override def copyDataSourceV2Relation(rel: DataSourceV2Relation, table: Table): DataSourceV2Relation = {
    rel.copy(table = table)
  }

  override def parseDataTypeJson(json: String): DataType = AdapterSupport330.parseDataTypeJson(json)

  override def mapPartitionsWithIndexInternal[T, U: ClassTag](
    rdd: RDD[T], f: (Int, Iterator[T]) => Iterator[U], isOrderSensitive: Boolean): RDD[U] = {
    AdapterSupport330.mapPartitionsWithIndexInternal(rdd, f, isOrderSensitive)
  }

  override def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    AdapterSupport330.createShuffleDependency(rdd, partitioner, serializer, writeMetrics)
  }

  override def createCatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String]): CatalogTable = CatalogTable(identifier, tableType, storage, schema, provider=provider)

  override def offsetAsJson(offset: Offset): String = offset match {
    case sv: SerializedOffset => sv.json
    case _ => throw new IllegalArgumentException("Unsupported offset format")
  }

  override def copyAggregate(agg: Aggregate, child: LogicalPlan): Aggregate = agg.copy(child = child)

  override def driverMemoryOverheadMinMib(conf: SparkConf): Long = AdapterSupport330.memoryOverheadMinMib

  override def getResourcesForClusterManager(
    resourceProfile: ResourceProfile,
    memoryOverheadFactor: Double,
    conf: SparkConf,
    isPythonApp: Boolean): AnyRef = {
    AdapterSupport330.getResourcesForClusterManager(resourceProfile, memoryOverheadFactor, conf, isPythonApp)
  }

  override def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String] = {
    AdapterSupport330.sparkJavaOpts(conf, filterKey)
  }

  override def wrapRestServlet(servlet: RestServletCompat): AnyRef = {
    RestServletSupport330.wrapRestServlet(servlet)
  }

  override def getHistoryProviderName(conf: SparkConf): String = AdapterSupport330.getHistoryProviderName(conf)

  override def createYtLogPage(delegate: YtLogPageDelegate): YtLogPage = AdapterSupport330.createYtLogPage(delegate)

  override def createServletParams(ytLogPageDelegate: YtLogPageDelegate): AnyRef = {
    AdapterSupport330.createServletParams(ytLogPageDelegate)
  }

  override def createColumnarBatchRowWrapper(
    row: InternalRow,
    getStructImpl: (Int, Int) => InternalRow,
    getArrayImpl: Int => ArrayData,
    getMapImpl: Int => MapData): InternalRow = {
    new ColumnarBatchRowWrapperBase(row, getStructImpl, getArrayImpl, getMapImpl) { }
  }

  override def getFileSourceScanExecStream(scan: FileSourceScanExec): Option[SparkDataStream] = {
    throw new UnsupportedOperationException("getStream in FileSourceScanExec is implemented starting from Spark 4.0.0")
  }

  override def getHadoopConf(sparkConf: SparkConf): Configuration = AdapterSupport330.getHadoopConf(sparkConf)

  override def fetchFile(url: String, targetDir: File, conf: SparkConf): File = {
    AdapterSupport330.fetchFile(url, targetDir, conf)
  }
}
