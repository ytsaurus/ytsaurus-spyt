package org.apache.spark.sql

import jakarta.servlet.http.HttpServletRequest
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{YtLogPage, YtLogPageDelegate}
import org.apache.spark.internal.config.{DRIVER_MIN_MEMORY_OVERHEAD, EXECUTOR_MIN_MEMORY_OVERHEAD, History}
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.classic.{ExpressionColumnNode, ExpressionUtils}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.ui.{JettyUtils, UIUtils}
import org.apache.spark.util.Utils
import org.json4s.JsonAST.JString

import java.io.File
import scala.reflect.ClassTag
import scala.xml.Node

// The sole purpose of this object is to increase visibility of some Spark package-private methods
object AdapterSupport400 {
  def dfShowString(df: Dataset[_], numRows: Int, truncate: Int): String = df.showString(numRows, truncate)

  def getSQLConf(sqlContext: SQLContext): SQLConf = {
    sqlContext.sparkSession.sessionState.conf
  }

  def createStreamingDataFrame(sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
    sqlContext.asInstanceOf[classic.SQLContext].internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  def createColumn(source: Seq[Column], constructor: Seq[Expression] => Expression): Column = {
    val sourceExpressions = source.map(c => ExpressionUtils.expression(c))
    val expression = constructor(sourceExpressions)
    ExpressionUtils.column(expression)
  }

  def applySchemaToPythonRDD(spark: SparkSession, rdd: RDD[Array[Any]], schemaString: String): DataFrame = {
    spark.applySchemaToPythonRDD(rdd, schemaString)
  }

  def parseDataTypeJson(json: String): DataType = {
    DataType.parseDataType(JString(json), fieldPath = "", collationsMap = Map.empty)
  }

  def mapPartitionsWithIndexInternal[T, U: ClassTag](
    rdd: RDD[T], f: (Int, Iterator[T]) => Iterator[U], isOrderSensitive: Boolean): RDD[U] = {
    rdd.mapPartitionsWithIndexInternal(f, isOrderSensitive = isOrderSensitive)
  }

  def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    new ShuffleDependency[K, V, C](rdd, partitioner, serializer,
      shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))
  }

  def driverMemoryOverheadMinMib(conf: SparkConf): Long = {
    conf.get(DRIVER_MIN_MEMORY_OVERHEAD)
  }

  def getResourcesForClusterManager(
    resourceProfile: ResourceProfile,
    memoryOverheadFactor: Double,
    conf: SparkConf,
    isPythonApp: Boolean): ResourceProfile.ExecutorResourcesOrDefaults = {
    ResourceProfile.getResourcesForClusterManager(
      resourceProfile.id,
      resourceProfile.executorResources,
      conf.get(EXECUTOR_MIN_MEMORY_OVERHEAD),
      memoryOverheadFactor,
      conf,
      isPythonApp,
      Map.empty)
  }

  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String] = {
    Utils.sparkJavaOpts(conf, filterKey)
  }

  def getHistoryProviderName(conf: SparkConf): String = {
    conf.get(History.PROVIDER)
  }

  def createYtLogPage(delegate: YtLogPageDelegate): YtLogPage = new YtLogPage(delegate) {
    override def render(request: HttpServletRequest): Seq[Node] = {
      val (content, title) = delegate.render(request.getParameter)
      UIUtils.basicSparkPage(request, content, title)
    }
  }

  def createServletParams(ytLogPageDelegate: YtLogPageDelegate): JettyUtils.ServletParams[String] = {
    (request: HttpServletRequest) => ytLogPageDelegate.renderLog(request.getParameter)
  }

  def getHadoopConf(sparkConf: SparkConf): Configuration = {
    SparkHadoopUtil.newConfiguration(sparkConf)
  }

  def fetchFile(url: String, targetDir: File, conf: SparkConf): File = Utils.fetchFile(
    url,
    targetDir,
    conf,
    SparkHadoopUtil.get.newConfiguration(conf),
    System.currentTimeMillis(),
    useCache = false
  )
}
