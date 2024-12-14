package org.apache.spark.sql.yt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v2.YtReaderOptions
import org.apache.spark.sql.vectorized.{ColumnarBatch, YtFileFormat}
import org.apache.spark.util.collection.BitSet
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

case class YtSourceScanExec(@transient relation: HadoopFsRelation,
                            output: Seq[Attribute],
                            requiredSchema: StructType,
                            partitionFilters: Seq[Expression],
                            optionalBucketSet: Option[BitSet],
                            dataFilters: Seq[Expression],
                            tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec {

  private lazy val optimizedForScan: Boolean = relation.fileFormat match {
    case yf: YtFileFormat =>
      relation.location.asInstanceOf[InMemoryFileIndex]
        .allFiles().forall { fileStatus =>
        YtHadoopPath.fromPath(fileStatus.getPath) match {
          case yp: YtHadoopPath => !yp.meta.isDynamic && yp.meta.optimizeMode == OptimizeMode.Scan
          case _ => false
        }
      }
    case _ => false
  }

  @transient
  private val wrappedRelation: HadoopFsRelation = relation.copy(
    options = relation.options + (YtTableSparkSettings.OptimizedForScan.name -> optimizedForScan.toString)
  )(relation.sparkSession)

  private val delegate = new FileSourceScanExecDelegate(
    wrappedRelation, output, requiredSchema, partitionFilters, optionalBucketSet, dataFilters, tableIdentifier
  )

  val maybeReadParallelism: Option[Int] = delegate.relation.options.get("readParallelism").map(_.toInt)

  override lazy val metadata: Map[String, String] = delegate.metadata

  override def inputRDDs(): Seq[RDD[InternalRow]] = delegate.inputRDDs()

  override protected def doExecute(): RDD[InternalRow] = {
    YtSourceScanExec.currentThreadInstance.set(this)
    val rdd = delegate.doExecuteInternal()
    YtSourceScanExec.currentThreadInstance.remove()
    rdd
  }

  override lazy val supportsColumnar: Boolean = delegate.supportsColumnar

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    YtSourceScanExec.currentThreadInstance.set(this)
    val rdd = delegate.doExecuteColumnarInternal()
    YtSourceScanExec.currentThreadInstance.remove()
    rdd
  }

  override def vectorTypes: Option[Seq[String]] = delegate.vectorTypes

  override def outputPartitioning: Partitioning = delegate.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = delegate.outputOrdering

  override def verboseStringWithOperatorId(): String = delegate.verboseStringWithOperatorId()

  override lazy val metrics: Map[String, SQLMetric] = delegate.metrics

  override val nodeNamePrefix: String = delegate.nodeNamePrefix

  override protected def doCanonicalize(): FileSourceScanExec = delegate.doCanonicalize()
}

object YtSourceScanExec {
  val currentThreadInstance: ThreadLocal[YtSourceScanExec] = new ThreadLocal[YtSourceScanExec]()
}

class FileSourceScanExecDelegate(relation: HadoopFsRelation,
                                 output: Seq[Attribute],
                                 requiredSchema: StructType,
                                 partitionFilters: Seq[Expression],
                                 optionalBucketSet: Option[BitSet],
                                 dataFilters: Seq[Expression],
                                 tableIdentifier: Option[TableIdentifier])
  extends FileSourceScanExec(relation, output, requiredSchema, partitionFilters, optionalBucketSet,
    None, dataFilters, tableIdentifier) {

  override lazy val supportsColumnar: Boolean = {
    relation.fileFormat match {
      case yf: YtFileFormat => YtReaderOptions.supportBatch(
        SparkAdapter.instance.fromAttributes(output), relation.options, relation.sparkSession.sqlContext.conf
      )
      case f => f.supportBatch(relation.sparkSession, SparkAdapter.instance.fromAttributes(output))
    }
  }

  def doExecuteInternal(): RDD[InternalRow] = this.doExecute()
  def doExecuteColumnarInternal(): RDD[ColumnarBatch] = this.doExecuteColumnar()
}
