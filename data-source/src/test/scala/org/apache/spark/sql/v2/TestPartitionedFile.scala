package org.apache.spark.sql.v2

import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate.YtPartitionedFileExt
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.fs.{YtHadoopPath, YtTableMeta}

sealed trait TestPartitionedFile {
  def toPartitionedFile: PartitionedFile
}

object TestPartitionedFile {
  def fromPartitionedFile(file: PartitionedFile): TestPartitionedFile = {
    file match {
      case ytFile: YtPartitionedFileExt if !ytFile.delegate.isDynamic =>
        Static(ytFile.path, ytFile.delegate.beginRow, ytFile.delegate.endRow)
      case ytFile: YtPartitionedFileExt =>
        Dynamic(ytFile.path, ytFile.length)
      case _ => Csv(file.filePath, file.length)
    }
  }

  case class Static(path: String, beginRow: Long, endRow: Long) extends TestPartitionedFile {
    override def toPartitionedFile: PartitionedFile =
      YtPartitionedFileDelegate.static(path, beginRow, endRow, 0L, YtPartitionedFileDelegate.emptyInternalRow,
        YtHadoopPath(YPathEnriched.fromString(path), YtTableMeta()))
  }

  case class Dynamic(path: String, length: Long) extends TestPartitionedFile {
    override def toPartitionedFile: PartitionedFile =
      YtPartitionedFileDelegate.dynamic(path, YtPartitionedFileDelegate.fullRange, length,
        YtPartitionedFileDelegate.emptyInternalRow,YtHadoopPath(YPathEnriched.fromString(path),
          YtTableMeta(isDynamic = true)))
  }

  case class Csv(path: String, length: Long) extends TestPartitionedFile {
    override def toPartitionedFile: PartitionedFile =
      PartitionedFile(YtPartitionedFileDelegate.emptyInternalRow, path, 0, length)
  }
}
