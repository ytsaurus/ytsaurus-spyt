package tech.ytsaurus.spyt

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Category, Level}
import org.apache.spark.executor.{ExecutorBackendFactory, ExecutorBackendFactory330}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, FileScanBuilder, PushDownUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.v2.{ScanBuilderAdapter, YtScanBuilder330}
import org.apache.spark.sql.Utils330
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.format.YtPartitioningSupport.YtPartitionedFileBase
import tech.ytsaurus.spyt.format.{YtPartitionedFile330, YtPartitioningDelegate}

@MinSparkVersion("3.3.0")
class SparkAdapterBase330 extends SparkAdapterBase {

  override def createYtScanOutputPartitioning(nPartitions: Int): Partitioning = {
    // TODO consider using org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning
    new UnknownPartitioning(nPartitions)
  }

  override def createYtScanBuilder(scanBuilderAdapter: ScanBuilderAdapter): FileScanBuilder = {
    new YtScanBuilder330(scanBuilderAdapter)
  }

  override def checkPushedFilters(scanBuilder: ScanBuilder,
                                  filters: Seq[Expression],
                                  expected: Seq[Filter]): (Seq[Any], Seq[Any]) = {
    val filtersOrPredicates =  PushDownUtils.pushFilters(scanBuilder, filters)._1
    filtersOrPredicates match {
      case Left(filters) => (filters, expected)
      case Right(predicates) => (predicates, expected.map(Utils330.filterToPredicate))
    }
  }

  override def executorBackendFactory: ExecutorBackendFactory = ExecutorBackendFactory330

  override def parserUtilsWithOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    ParserUtils.withOrigin(ctx)(f)
  }

  override def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition = {
    dsrddPartition.inputPartitions.head
  }

  override def createLoggingEvent(fqnOfCategoryClass: String, logger: Category,
                                  timeStamp: Long, level: Level,
                                  message: String, throwable: Throwable): LoggingEvent = {
    Utils330.createLoggingEvent(fqnOfCategoryClass, logger, timeStamp, level, message, throwable)
  }
}
