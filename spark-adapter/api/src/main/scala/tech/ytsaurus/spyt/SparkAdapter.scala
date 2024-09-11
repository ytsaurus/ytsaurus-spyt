package tech.ytsaurus.spyt

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.log4j.{Category, Level}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.executor.ExecutorBackendFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, FileScanBuilder}
import org.apache.spark.sql.sources
import org.apache.spark.sql.v2.ScanBuilderAdapter
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.YtPartitioningDelegate
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase

import java.util.ServiceLoader
import scala.collection.JavaConverters._

trait SparkAdapter {
  def minSparkVersion: String
  def createYtScanOutputPartitioning(numPartitions: Int): Partitioning
  def createYtScanBuilder(scanBuilderAdapter: ScanBuilderAdapter): FileScanBuilder
  def executorBackendFactory: ExecutorBackendFactory
  def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T]
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

object SparkAdapter {

  private val log = LoggerFactory.getLogger(getClass)

  lazy val instance: SparkAdapter = loadInstance()

  private def loadInstance(): SparkAdapter = {
    log.debug(s"Runtime Spark version: ${SparkVersionUtils.currentVersion}")
    val instances = ServiceLoader.load(classOf[SparkAdapter]).asScala
    log.debug(s"Num of available SparkAdapter instances: ${instances.size}")
    val instance = instances.filter { i =>
      SparkVersionUtils.ordering.lteq(i.minSparkVersion, SparkVersionUtils.currentVersion)
    }.toList.sortBy(_.minSparkVersion)(SparkVersionUtils.ordering.reverse).head
    log.debug(s"Runtime Spark version: ${SparkVersionUtils.currentVersion}, using SparkAdapter for ${instance.minSparkVersion}")
    instance
  }
}
