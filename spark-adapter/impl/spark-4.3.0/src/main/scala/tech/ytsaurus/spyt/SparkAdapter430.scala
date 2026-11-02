package tech.ytsaurus.spyt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AdapterSupport430
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{FileSourceScanExec, FileSourceScanExecDelegate}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

trait SparkAdapter430 extends SparkAdapter {

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

  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation = {
    rel.copy(scan = newScan)
  }

  override def checkAndGlobPathIfNecessary(paths: Seq[String], hadoopConf: Configuration): Seq[Path] = {
    AdapterSupport430.checkAndGlobPathIfNecessary(paths, hadoopConf)
  }
}
