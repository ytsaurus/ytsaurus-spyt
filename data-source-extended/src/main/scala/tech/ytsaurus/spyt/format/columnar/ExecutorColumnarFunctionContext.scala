package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.SparkFiles

import java.nio.file.{Files, Paths}

/** Supplies a partition allocator and executor-local artifact paths to plugin evaluators.
 *
 * Construct with the driver's captured session directory; the caller retains ownership of the allocator.
 */
private[columnar] class ExecutorColumnarFunctionContext(
  partitionAllocator: RootAllocator,
  artifactDirectory: String) extends ColumnarFunctionContext {

  /** Supplies the partition allocator; evaluators must close their allocations before task cleanup. */
  override def allocator(): RootAllocator = partitionAllocator

  /** Resolves a bare artifact filename in the captured session directory, then shared Spark files.
   *
   * Plugins call this on executors rather than serializing a driver-local absolute library path.
   */
  override def resolveArtifact(filename: String): String = {
    val validFilename = Option(filename).exists { name =>
      !Set("", ".", "..").contains(name) && !name.exists(c => c == '/' || c == '\\')
    }
    require(validFilename, "Artifact must be a filename without directories")
    val sessionPath = Paths.get(SparkFiles.get(Paths.get(artifactDirectory, filename).toString))
    val sharedPath = Paths.get(SparkFiles.get(filename))
    val path = if (Files.isRegularFile(sessionPath)) sessionPath else sharedPath
    require(Files.isRegularFile(path),
      s"Distributed artifact $filename was not found in $sessionPath or $sharedPath")
    path.toString
  }
}
