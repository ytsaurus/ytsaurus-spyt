package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.{CheckPermission, GetCurrentUser}
import tech.ytsaurus.rpcproxy.ESecurityAction
import tech.ytsaurus.spyt.fs.YtTableFileSystem.{DEFAULT_FILTER, PathName}
import tech.ytsaurus.spyt.fs.path._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.cypress.{PathType, YtAttributes}
import tech.ytsaurus.spyt.wrapper.table.{OptimizeMode, TableType}
import tech.ytsaurus.ysontree.YTreeNode

import scala.collection.JavaConverters._
import scala.language.postfixOps

@SerialVersionUID(1L)
class YtTableFileSystem extends YtFileSystemBase {
  private val log = LoggerFactory.getLogger(getClass)

  private def setupContext(f: Path): (YPathEnriched, Map[String, YTreeNode], CompoundClient) = {
    val path = YPathEnriched.fromPath(f)
    val yPath = path.toYPath
    implicit val yt: CompoundClient = ytClient(path)
    val attributes = convertExceptions(YtWrapper.attributes(yPath, path.transaction))
    (path, attributes, yt)
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    listStatus(f, DEFAULT_FILTER)
  }

  override def listStatus(f: Path, filter: PathFilter): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    val (path, attributes, yt) = setupContext(f)
    listStatus(path, expandDirectory = true, attributes, filter)(yt)
  }

  private def listStatus(path: YPathEnriched, expandDirectory: Boolean, attributes: Map[String, YTreeNode], filter: PathFilter)
    (implicit yt: CompoundClient): Array[FileStatus] = {
    PathType.fromAttributes(attributes) match {
      case PathType.File => Array(getFileStatus(path.lock(), attributes))
      case PathType.Table => Array(lockTable(path, attributes))
      case PathType.Directory => if (expandDirectory) listYtDirectory(path, filter) else Array(getFileStatus(path, attributes))
      case pathType => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  private def listYtDirectory(path: YPathEnriched, filter: PathFilter)(implicit yt: CompoundClient): Array[FileStatus] = {
    YtWrapper.listDir(path.toYPath, path.transaction, YtAttributes.tableAttributes).flatMap { nameWithAttrs =>
      val name = nameWithAttrs.stringValue()
      if (filter.accept(new PathName(name))) {
        val attributes: Map[String, YTreeNode] = nameWithAttrs.getAttributes.asScala.toMap
        listStatus(path.child(name), expandDirectory = false, attributes, filter)
      } else {
        Nil
      }
    }.toArray
  }

  private def lockTable(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): FileStatus = {
    YtWrapper.tableType(attributes) match {
      case TableType.Static => lockStaticTable(path, attributes)
      case TableType.Dynamic => lockDynamicTable(path, attributes)
    }
  }

  private lazy val isDriver: Boolean = SparkSession.getDefaultSession.nonEmpty

  private def lockStaticTable(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): FileStatus = {
    getFileStatus(path.dropTimestamp().lock(), attributes)
  }

  private def lockDynamicTable(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): FileStatus = {
    if (path.timestamp.isDefined || path.transaction.isDefined) {
      getFileStatus(path.lock(), attributes)
    } else {
      if (!isDriver) {
        log.warn("Generating timestamps of dynamic tables on executors causes reading files with different timestamps")
      }
      val ts = YtWrapper.maxAvailableTimestamp(path.toYPath)
      getFileStatus(path.withTimestamp(ts), attributes)
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    val (path, attributes, yt) = setupContext(f)
    getFileStatus(f, path, attributes)(yt)
  }

  private def getFileStatus(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): FileStatus = {
    getFileStatus(path.toPath, path, attributes)
  }

  private def getFileStatus(f: Path, path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): FileStatus = {
    log.debugLazy(s"Get file status $f")

    val modificationTime = YtWrapper.modificationTimeTs(attributes)
    YtWrapper.pathType(attributes) match {
      case PathType.File => buildFileStatus(f, attributes, modificationTime)

      case PathType.Table => buildTableStatus(path, attributes, modificationTime)

      case PathType.Directory => new FileStatus(0L, true, 1, 0L, modificationTime, f)

      case PathType.None => null
    }
  }

  private def buildFileStatus(f: Path,
    attributes: Map[String, YTreeNode],
    modificationTime: Long
  ): FileStatus = {
    val size = YtWrapper.fileSize(attributes)
    new FileStatus(size, false, 1, size, modificationTime, f)
  }

  private def buildTableStatus(path: YPathEnriched,
    attributes: Map[String, YTreeNode],
    modificationTime: Long
  )(implicit yt: CompoundClient): FileStatus = {
    val size = YtWrapper.fileSize(attributes) max 1L // size may be 0 when real data exists
    val optimizeMode = YtWrapper.optimizeMode(attributes)

    val tableMeta = YtWrapper.tableType(attributes) match {
      case TableType.Static =>
        buildStaticTableMeta(path, attributes, size, modificationTime, optimizeMode)

      case TableType.Dynamic =>
        buildDynamicTableMeta(attributes, size, modificationTime, optimizeMode)
    }

    val richPath = YtHadoopPath(path, tableMeta)
    YtFileStatus.toFileStatus(richPath)
  }

  private def buildStaticTableMeta(path: YPathEnriched,
    attributes: Map[String, YTreeNode],
    size: Long,
    modificationTime: Long,
    optimizeMode: OptimizeMode
  )(implicit yt: CompoundClient): YtTableMeta = {
    val directRowCount = YtWrapper.rowCount(attributes)
    val resolvedRowCount = directRowCount.getOrElse(YtWrapper.chunkRowCount(attributes))

    val fullReadAllowed =
      directRowCount match {
        case Some(_) => true
        case None => isFullReadAllowed(path)
      }

    YtTableMeta(
      rowCount = resolvedRowCount,
      size = size,
      modificationTime = modificationTime,
      optimizeMode = optimizeMode,
      fullReadAllowed = fullReadAllowed,
      schemaIdOpt = YtWrapper.schemaId(attributes)
    )
  }

  private def buildDynamicTableMeta(
    attributes: Map[String, YTreeNode],
    size: Long,
    modificationTime: Long,
    optimizeMode: OptimizeMode
  ): YtTableMeta = {
    YtTableMeta(
      rowCount = YtWrapper.chunkRowCount(attributes),
      size = size,
      modificationTime = modificationTime,
      optimizeMode = optimizeMode,
      isDynamic = true,
      schemaIdOpt = YtWrapper.schemaId(attributes)
    )
  }

  private val FullReadPermission = 0x2000

  private def isFullReadAllowed(path: YPathEnriched)(implicit yt: CompoundClient): Boolean = {
    val user = Option(ytUser).getOrElse(yt.getCurrentUser(GetCurrentUser.builder().build()).join())

    val request = CheckPermission.builder()
      .setUser(user)
      .setPath(path.toYPath.toString)
      .setPermissions(FullReadPermission)
      .setColumns(null)
      .build()

    val result = yt.checkPermission(request).join()

    result.getAction == ESecurityAction.SA_ALLOW
  }

}

object YtTableFileSystem {
  val DEFAULT_FILTER: PathFilter = _ => true

  private class PathName(name: String) extends Path("unused") {
    override def getName: String = name
  }
}
