package org.apache.spark.sql.v2

import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.format.YtPartitionedFile
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.fs.{YtHadoopPath, YtTableMeta}

sealed trait TestPartitionedFile {
  def toPartitionedFile: PartitionedFile
}

object TestPartitionedFile {
  def fromPartitionedFile(file: PartitionedFile): TestPartitionedFile = {
    file match {
      case ytFile: YtPartitionedFile if !ytFile.isDynamic =>
        Static(ytFile.path, ytFile.beginRow, ytFile.endRow)
      case ytFile: YtPartitionedFile =>
        Dynamic(ytFile.path, ytFile.length)
      case _ => Csv(file.filePath, file.length)
    }
  }

  case class Static(path: String, beginRow: Long, endRow: Long) extends TestPartitionedFile {
    override def toPartitionedFile: PartitionedFile =
      YtPartitionedFile.static(path, beginRow, endRow, 0L, YtPartitionedFile.emptyInternalRow,
        YtHadoopPath(YPathEnriched.fromString(path), YtTableMeta()))
  }

  case class Dynamic(path: String, length: Long) extends TestPartitionedFile {
    override def toPartitionedFile: PartitionedFile =
      YtPartitionedFile.dynamic(path, YtPartitionedFile.fullRange, length, YtPartitionedFile.emptyInternalRow,
        YtHadoopPath(YPathEnriched.fromString(path), YtTableMeta(isDynamic = true)))
  }

  case class Csv(path: String, length: Long) extends TestPartitionedFile {
    override def toPartitionedFile: PartitionedFile =
      PartitionedFile(YtPartitionedFile.emptyInternalRow, path, 0, length)
  }
}
