package tech.ytsaurus.spyt.format

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

class DelegatingOutputCommitProtocol(jobId: String,
                                     outputPath: String,
                                     dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  import DelegatingOutputCommitProtocol._

  private val delegate: FileCommitProtocol = {
    if (isYtsaurusFileSystem(outputPath)) {
      new YtOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite)
    } else {
      new SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite)
    }
  }

  override def setupJob(jobContext: JobContext): Unit =
    delegate.setupJob(jobContext)

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit =
    delegate.commitJob(jobContext, taskCommits)

  override def abortJob(jobContext: JobContext): Unit =
    delegate.abortJob(jobContext)

  override def setupTask(taskContext: TaskAttemptContext): Unit =
    delegate.setupTask(taskContext)

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String =
    delegate.newTaskTempFile(taskContext, dir, ext)

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String =
    delegate.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage =
    delegate.commitTask(taskContext)

  override def abortTask(taskContext: TaskAttemptContext): Unit =
    delegate.abortTask(taskContext)

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean =
    delegate.deleteWithJob(fs, path, recursive)
}

object DelegatingOutputCommitProtocol {
  def isYtsaurusFileSystem(outputPath: String): Boolean = {
    val schemaPos = outputPath.indexOf(":/")
    if (schemaPos < 0) {
      return false
    }
    val schema = outputPath.substring(0, schemaPos)
    ytsaurusFileSystems.contains(schema)
  }

  private val ytsaurusFileSystems: Set[String] = Set("yt", "ytCached", "ytEventLog", "ytTable")
}