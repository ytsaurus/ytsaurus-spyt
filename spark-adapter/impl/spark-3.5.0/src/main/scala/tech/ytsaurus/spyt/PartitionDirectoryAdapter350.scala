package tech.ytsaurus.spyt
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.execution.datasources.PartitionDirectory

@MinSparkVersion("3.5.0")
class PartitionDirectoryAdapter350 extends PartitionDirectoryAdapter {
  override def getPartitionFileStatuses(pd: PartitionDirectory): Seq[FileStatus] = pd.files.map(_.fileStatus)
}
