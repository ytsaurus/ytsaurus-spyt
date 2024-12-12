package tech.ytsaurus.spyt

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Category, Level}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.Partitioner
import org.apache.spark.executor.ExecutorBackendFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, ScanBuilder}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, FileScanBuilder}
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
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

private class SparkAdapterImpl(base: SparkAdapterBase,
                               pfAdapter: PartitionedFileAdapter,
                               pfPathAdapter: PartitionedFilePathAdapter,
                               ytprfc: YtPartitionReaderFactoryCreator,
                               shuffleAdapter: ShuffleAdapter,
                               resourcesAdapter: ResourcesAdapter
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
