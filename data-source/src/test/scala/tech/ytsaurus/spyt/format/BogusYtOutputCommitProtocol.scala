package tech.ytsaurus.spyt.format

import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol

import scala.util.Random

class BogusYtOutputCommitProtocol(jobId: String, outputPath: String, dynamicPartitionOverwrite: Boolean)
  extends DelegatingOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  import BogusYtOutputCommitProtocol._

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    val res = super.commitTask(taskContext)
    if (pseudoRandom.nextInt(5) == 0) {
      throw new RuntimeException("BOOOOM!!!!")
    }
    res
  }
}

object BogusYtOutputCommitProtocol {
  // We need a global pseudo-random sequence here so the test would be deterministic
  // With seed of 3 we expect that commitTask will fail 3 times
  val pseudoRandom = new Random(3)
}

class BogusYtOutputJobCommitProtocol(jobId: String, outputPath: String, dynamicPartitionOverwrite: Boolean)
  extends DelegatingOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    super.commitJob(jobContext, taskCommits)
    throw new RuntimeException("BOOOOM!!!!")
  }
}
