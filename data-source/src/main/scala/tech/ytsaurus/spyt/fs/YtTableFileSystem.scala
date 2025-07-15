package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.fs.path._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.cypress.PathType
import tech.ytsaurus.spyt.wrapper.table.TableType
import tech.ytsaurus.ysontree.YTreeNode

import java.io.FileNotFoundException
import scala.language.postfixOps

@SerialVersionUID(1L)
class YtTableFileSystem extends YtFileSystemBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    listStatus(YPathEnriched.fromPath(f), expandDirectory = true)
  }

  private def listStatus(path: YPathEnriched, expandDirectory: Boolean)
                        (implicit yt: CompoundClient = ytClient(path)): Array[FileStatus] = {
    val yPath = path.toYPath
    if (!YtWrapper.exists(yPath, path.transaction)) {
      throw new FileNotFoundException(path.toStringPath)
    }
    val attributes = YtWrapper.attributes(yPath, path.transaction)
    PathType.fromAttributes(attributes) match {
      case PathType.File => Array(getFileStatus(path.lock()))
      case PathType.Table => Array(lockTable(path, attributes))
      case PathType.Directory => if (expandDirectory) listYtDirectory(path) else Array(getFileStatus(path))
      case pathType => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  private def listYtDirectory(path: YPathEnriched)(implicit yt: CompoundClient): Array[FileStatus] = {
    YtWrapper.listDir(path.toYPath, path.transaction).flatMap { name => listStatus(path.child(name), expandDirectory = false) }
  }

  private def lockTable(path: YPathEnriched, attributes: Map[String, YTreeNode])
                       (implicit yt: CompoundClient): FileStatus = {
    YtWrapper.tableType(attributes) match {
      case TableType.Static => lockStaticTable(path)
      case TableType.Dynamic => lockDynamicTable(path)
    }
  }

  private lazy val isDriver: Boolean = SparkSession.getDefaultSession.nonEmpty

  private def lockStaticTable(path: YPathEnriched)(implicit yt: CompoundClient): FileStatus = {
    getFileStatus(path.dropTimestamp().lock())
  }

  private def lockDynamicTable(path: YPathEnriched)(implicit yt: CompoundClient): FileStatus = {
    if (path.timestamp.isDefined || path.transaction.isDefined) {
      getFileStatus(path.lock())
    } else {
      if (!isDriver) {
        log.warn("Generating timestamps of dynamic tables on executors causes reading files with different timestamps")
      }
      val ts = YtWrapper.maxAvailableTimestamp(path.toYPath)
      getFileStatus(path.withTimestamp(ts))
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    getFileStatus(f, YPathEnriched.fromPath(f))
  }

  private def getFileStatus(path: YPathEnriched)(implicit yt: CompoundClient): FileStatus = {
    getFileStatus(path.toPath, path)
  }

  private def getFileStatus(f: Path, path: YPathEnriched)(implicit yt: CompoundClient = ytClient(path)): FileStatus = {
    log.debugLazy(s"Get file status $f")
    val yPath = path.toYPath
    if (!YtWrapper.exists(yPath, path.transaction)) {
      throw new FileNotFoundException(s"File $f is not found")
    }
    val attributes = YtWrapper.attributes(yPath, path.transaction)(yt)
    val modificationTime = YtWrapper.modificationTimeTs(attributes)
    YtWrapper.pathType(attributes) match {
      case PathType.File =>
        val size = YtWrapper.fileSize(attributes)
        new FileStatus(size, false, 1, size, modificationTime, f)
      case PathType.Table =>
        val size = YtWrapper.fileSize(attributes) max 1L // NB: Size may be 0 when a real data exists
        val optimizeMode = YtWrapper.optimizeMode(attributes)
        val (rowCount, isDynamic) = YtWrapper.tableType(attributes) match {
          case TableType.Static => (YtWrapper.rowCount(attributes), false)
          case TableType.Dynamic => (YtWrapper.chunkRowCount(attributes), true)
        }
        val richPath = YtHadoopPath(path, YtTableMeta(rowCount, size, modificationTime, optimizeMode, isDynamic))
        YtFileStatus.toFileStatus(richPath)
      case PathType.Directory => new FileStatus(0, true, 1, 0, modificationTime, f)
      case PathType.None => null
    }
  }
}
