package tech.ytsaurus.spyt.format

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Write
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.config.SparkYtSparkConf

class DelegatingOutputCommitProtocol(jobId: String,
                                     outputPath: String,
                                     dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  import DelegatingOutputCommitProtocol._

  @transient private var isTable: Boolean = _
  private lazy val delegate: FileCommitProtocol = createDelegate()

  private def createDelegate(): FileCommitProtocol = {
    if (!isYtsaurusFileSystem(outputPath)) {
      return new SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite)
    }

    implicit val ytClient: CompoundClient = YtOutputCommitProtocol.cachedClient

    if (YtWrapper.isDynamicTable(outputPath)) {
      new DynamicTableOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite)
    } else if (isDistributedWrite(SparkEnv.get.conf)) {
      new DistributedWriteOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite)
    } else {
      new YtOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite)
    }
  }

  private def isDistributedWrite(conf: SparkConf): Boolean = {
    conf.getYtConf(Write.Distributed.Enabled).get && isTable
  }

  override def setupJob(jobContext: JobContext): Unit = {
    isTable = YtTableSparkSettings.isTable(jobContext.getConfiguration)
    delegate.setupJob(jobContext)
  }

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

  override def onTaskCommit(taskCommit: FileCommitProtocol.TaskCommitMessage): Unit =
    delegate.onTaskCommit(taskCommit)
}

object DelegatingOutputCommitProtocol {
  private def isYtsaurusFileSystem(outputPath: String): Boolean = {
    val schemaPos = outputPath.indexOf(":/")
    if (schemaPos < 0) {
      // Output paths without specified schema should be treated as ytTable,
      // because spark.hadoop.fs.null.impl property is set to tech.ytsaurus.spyt.fs.YtTableFileSystem in
      // default SPYT configuration for Spark SQL compatibility with other YTsaurus SQL engines.
      return true
    }
    val schema = outputPath.substring(0, schemaPos)
    ytsaurusFileSystems.contains(schema)
  }

  private val ytsaurusFileSystems: Set[String] = Set("yt", "ytCached", "ytEventLog", "ytTable")
}
