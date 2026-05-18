package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{YtLogPage, YtLogPageDelegate}
import org.apache.spark.internal.config.History
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, NumericType, StructType}
import org.apache.spark.ui.{JettyUtils, UIUtils}
import org.apache.spark.util.Utils
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import org.json4s.JsonAST.JString

import java.io.File
import javax.servlet.http.HttpServletRequest
import scala.reflect.ClassTag
import scala.xml.Node

// The sole purpose of this object is to increase visibility of some Spark package-private methods
object AdapterSupport330 {
  def getExecutorCores(execResources: Product): Int = execResources.asInstanceOf[ExecutorResourcesOrDefaults].cores
  def schemaToAttributes(schema: StructType): Seq[AttributeReference] = schema.toAttributes
  def castToLong(x: NumericType): Any => Any = { b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b) }
  def fromAttributes(attributes: Seq[Attribute]): StructType = StructType.fromAttributes(attributes)
  def dfShowString(df: Dataset[_], numRows: Int, truncate: Int): String = df.showString(numRows, truncate)
  def createAnalysisException(message: String): AnalysisException = new AnalysisException(message)
  def getSQLConf(sqlContext: SQLContext): SQLConf = sqlContext.conf

  def createStreamingDataFrame( sqlContext: SQLContext, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  def sparkImplicits(spark: SparkSession): SQLImplicits = spark.implicits

  def applySchemaToPythonRDD(spark: SparkSession, rdd: RDD[Array[Any]], schemaString: String): DataFrame = {
    spark.applySchemaToPythonRDD(rdd, schemaString)
  }

  def parseDataTypeJson(json: String): DataType = DataType.parseDataType(JString(json))

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

  val memoryOverheadMinMib: Long = ResourceProfile.MEMORY_OVERHEAD_MIN_MIB

  def getResourcesForClusterManager(
    resourceProfile: ResourceProfile,
    memoryOverheadFactor: Double,
    conf: SparkConf,
    isPythonApp: Boolean): ResourceProfile.ExecutorResourcesOrDefaults = {
    ResourceProfile.getResourcesForClusterManager(
      resourceProfile.id,
      resourceProfile.executorResources,
      memoryOverheadFactor,
      conf,
      isPythonApp,
      Map.empty)
  }

  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String] = {
    Utils.sparkJavaOpts(conf, filterKey)
  }

  def getHistoryProviderName(conf: SparkConf): String = {
    conf.get(History.PROVIDER).getOrElse("org.apache.spark.deploy.history.FsHistoryProvider")
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
