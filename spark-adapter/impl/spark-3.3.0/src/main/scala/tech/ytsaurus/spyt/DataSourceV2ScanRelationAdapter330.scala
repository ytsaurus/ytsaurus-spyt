package tech.ytsaurus.spyt

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

@MinSparkVersion("3.3.0")
class DataSourceV2ScanRelationAdapter330 extends DataSourceV2ScanRelationAdapter {
  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation = {
    rel.copy(scan = newScan)
  }
}
