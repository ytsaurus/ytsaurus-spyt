package tech.ytsaurus.spyt

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Category, Level}
import org.apache.spark.executor.{ExecutorBackendFactory, ExecutorBackendFactory322}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.connector.read.partitioning.{Distribution, Partitioning}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, FileScanBuilder, PushDownUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.v2.{ScanBuilderAdapter, YtScanBuilder322, YtScanPartitioning}
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase
import tech.ytsaurus.spyt.format.{YtPartitionedFile322, YtPartitioningDelegate}

class SparkAdapter322 extends SparkAdapter {

  override def minSparkVersion: String = "3.2.2"

  override def createYtScanOutputPartitioning(nPartitions: Int): Partitioning = YtScanPartitioning(nPartitions)

  override def createYtScanBuilder(scanBuilderAdapter: ScanBuilderAdapter): FileScanBuilder = {
    new YtScanBuilder322(scanBuilderAdapter)
  }

  override def executorBackendFactory: ExecutorBackendFactory = ExecutorBackendFactory322

  override def createPartitionedFile(partitionValues: InternalRow, filePath: String,
                                     start: Long, length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, filePath, start, length)
  }

  override def createYtPartitionedFile[T <: YtPartitioningDelegate](delegate: T): YtPartitionedFileBase[T] = {
    new YtPartitionedFile322(delegate)
  }

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
}
