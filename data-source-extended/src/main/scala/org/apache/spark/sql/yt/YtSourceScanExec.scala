package org.apache.spark.sql.yt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v2.{YtFilePartition, YtReaderOptions}
import org.apache.spark.sql.vectorized.YtFileFormat
import org.apache.spark.util.collection.BitSet
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

  lazy val optimizedForScan: Boolean = relation.fileFormat match {
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

  lazy val relationOptions: Map[String, String] = relation.options + (
    YtTableSparkSettings.OptimizedForScan.name -> optimizedForScan.toString
    )

  private val delegate: FileSourceScanExec = new FileSourceScanExec(
    relation, output, requiredSchema, partitionFilters, optionalBucketSet, None, dataFilters, tableIdentifier
  ) {
    override lazy val supportsColumnar: Boolean = {
      relation.fileFormat match {
        case yf: YtFileFormat => YtReaderOptions.supportBatch(
          StructType.fromAttributes(output), relationOptions, relation.sparkSession.sqlContext.conf
        )
        // TODO Pay attention here for spark 3.4.x and above
        case f => f.supportBatch(relation.sparkSession, StructType.fromAttributes(output))
      }
    }
  }

  val maybeReadParallelism: Option[Int] = relation.options.get("readParallelism").map(_.toInt)

  override lazy val metadata: Map[String, String] = delegate.metadata

  override def inputRDDs(): Seq[RDD[InternalRow]] = delegate.inputRDDs()

  override protected def doExecute(): RDD[InternalRow] = {
    YtSourceScanExec.currentThreadInstance.set(this)
    val rdd = delegate.execute()
    YtSourceScanExec.currentThreadInstance.remove()
    rdd
  }
}

object YtSourceScanExec {
  val currentThreadInstance: ThreadLocal[YtSourceScanExec] = new ThreadLocal[YtSourceScanExec]()
}
