package tech.ytsaurus.spyt

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.PartitionedFile

@MinSparkVersion("3.2.2")
class PartitionedFilePathAdapter322 extends PartitionedFilePathAdapter {
  override def getStringFilePath(pf: PartitionedFile): String = pf.filePath

  override def getHadoopFilePath(pf: PartitionedFile): Path = new Path(pf.filePath)
}
