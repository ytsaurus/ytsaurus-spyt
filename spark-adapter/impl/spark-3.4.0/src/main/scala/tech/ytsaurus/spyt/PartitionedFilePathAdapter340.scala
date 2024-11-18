package tech.ytsaurus.spyt

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.PartitionedFile

@MinSparkVersion("3.4.0")
class PartitionedFilePathAdapter340 extends PartitionedFilePathAdapter {
  override def getStringFilePath(pf: PartitionedFile): String = pf.filePath.urlEncoded

  override def getHadoopFilePath(pf: PartitionedFile): Path = pf.filePath.toPath
}
