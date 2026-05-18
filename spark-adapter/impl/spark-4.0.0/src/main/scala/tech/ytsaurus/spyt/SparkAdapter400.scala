package tech.ytsaurus.spyt

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{YtLogPage, YtLogPageDelegate}
import org.apache.spark.deploy.rest.{RestServletCompat, RestServletSupport400}
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanExecDelegate}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatchRowWrapperBase
import org.apache.spark.unsafe.types.VariantVal
import org.apache.spark.util.collection.BitSet
import org.json4s.JString

import java.io.File
import scala.reflect.ClassTag

trait SparkAdapter400 extends SparkAdapter {

  override def dfShowString(df: Dataset[_], numRows: Int, truncate: Int): String = {
    AdapterSupport400.dfShowString(df, numRows, truncate)
  }

  override def createAnalysisException(message: String): AnalysisException = {
    new AnalysisExceptionImpl(message)
  }

  override def getSQLConf(sqlContext: SQLContext): SQLConf = AdapterSupport400.getSQLConf(sqlContext)

  override def createStreamingDataFrame(
    sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
    AdapterSupport400.createStreamingDataFrame(sqlContext, rdd, schema)
  }

  override def sparkImplicits(spark: SparkSession): SQLImplicits = spark.implicits

  override def createFileSourceScanExecDelegate(
    relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    supportsColumnarProvider: HadoopFsRelation => Boolean): FileSourceScanExec with FileSourceScanExecDelegate = {

      new FileSourceScanExec(relation, None, output, requiredSchema, partitionFilters, optionalBucketSet,
        None, dataFilters, tableIdentifier) with FileSourceScanExecDelegate {

        override lazy val supportsColumnar: Boolean = supportsColumnarProvider(relation)
      }
  }

  override def createColumn(source: Seq[Column], constructor: Seq[Expression] => Expression): Column = {
    AdapterSupport400.createColumn(source, constructor)
  }

  override def applySchemaToPythonRDD(spark: SparkSession, rdd: RDD[Array[Any]], schemaString: String): DataFrame = {
    AdapterSupport400.applySchemaToPythonRDD(spark, rdd, schemaString)
  }

  override def createStaticInvoke(
    clazz: Class[_], returnType: DataType, methodName: String, args: Seq[Expression]): Expression = {
    StaticInvoke(clazz, returnType, methodName, args, returnNullable = false)
  }

  override def parseDataTypeJson(json: String): DataType = AdapterSupport400.parseDataTypeJson(json)

  override def mapPartitionsWithIndexInternal[T, U: ClassTag](
    rdd: RDD[T], f: (Int, Iterator[T]) => Iterator[U], isOrderSensitive: Boolean): RDD[U] = {
    AdapterSupport400.mapPartitionsWithIndexInternal(rdd, f, isOrderSensitive)
  }

  override def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    AdapterSupport400.createShuffleDependency(rdd, partitioner, serializer, writeMetrics)
  }

  override def createCatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String]): CatalogTable = CatalogTable(identifier, tableType, storage, schema, provider=provider)

  override def copyAggregate(agg: Aggregate, child: LogicalPlan): Aggregate = agg.copy(child = child)

  override def driverMemoryOverheadMinMib(conf: SparkConf): Long = AdapterSupport400.driverMemoryOverheadMinMib(conf)

  override def getResourcesForClusterManager(
    resourceProfile: ResourceProfile,
    memoryOverheadFactor: Double,
    conf: SparkConf,
    isPythonApp: Boolean): AnyRef = {
    AdapterSupport400.getResourcesForClusterManager(resourceProfile, memoryOverheadFactor, conf, isPythonApp)
  }

  override def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String] = {
    AdapterSupport400.sparkJavaOpts(conf, filterKey)
  }

  override def wrapRestServlet(servlet: RestServletCompat): AnyRef = {
    RestServletSupport400.wrapRestServlet(servlet)
  }

  override def getHistoryProviderName(conf: SparkConf): String = AdapterSupport400.getHistoryProviderName(conf)

  override def createYtLogPage(delegate: YtLogPageDelegate): YtLogPage = AdapterSupport400.createYtLogPage(delegate)

  override def createServletParams(ytLogPageDelegate: YtLogPageDelegate): AnyRef = {
    AdapterSupport400.createServletParams(ytLogPageDelegate)
  }

  override def createColumnarBatchRowWrapper(
    row: InternalRow,
    getStructImpl: (Int, Int) => InternalRow,
    getArrayImpl: Int => ArrayData,
    getMapImpl: Int => MapData): InternalRow = {
    new ColumnarBatchRowWrapperBase(row, getStructImpl, getArrayImpl, getMapImpl) {
      override def getVariant(ordinal: Int): VariantVal = row.getVariant(ordinal)
    }
  }

  override def getFileSourceScanExecStream(scan: FileSourceScanExec): Option[SparkDataStream] = {
    scan.getStream
  }

  override def getHadoopConf(sparkConf: SparkConf): Configuration = AdapterSupport400.getHadoopConf(sparkConf)

  override def fetchFile(url: String, targetDir: File, conf: SparkConf): File = {
    AdapterSupport400.fetchFile(url, targetDir, conf)
  }
}
