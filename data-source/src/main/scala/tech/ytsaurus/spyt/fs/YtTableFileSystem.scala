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

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._


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
    listStatusAsync(path, expandDirectory = true, attributes, filter)(yt).join()
  }

  private def listStatusAsync(
    path: YPathEnriched,
    expandDirectory: Boolean,
    attributes: Map[String, YTreeNode],
    filter: PathFilter)(implicit yt: CompoundClient): CompletableFuture[Array[FileStatus]] = {

    PathType.fromAttributes(attributes) match {
      case PathType.File => path.lockAsync()
        .thenCompose(lockedPath => getFileStatus(lockedPath, attributes))
        .thenApply(fileStatus => Array(fileStatus))
      case PathType.Table =>
        lockTableAsync(path, attributes).thenApply(status => Array(status))
      case PathType.Directory => if (expandDirectory) {
          listYtDirectory(path, filter)
        } else {
          getFileStatus(path, attributes).thenApply(fileStatus => Array(fileStatus))
        }
      case pathType =>
        throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  private def listYtDirectory(path: YPathEnriched, filter: PathFilter)(implicit yt: CompoundClient): CompletableFuture[Array[FileStatus]] = {
    val childFutures = YtWrapper.listDir(path.toYPath, path.transaction, YtAttributes.tableAttributes).flatMap { nameWithAttrs =>
      val name = nameWithAttrs.stringValue()
      if (filter.accept(new PathName(name))) {
        val attributes: Map[String, YTreeNode] = nameWithAttrs.getAttributes.asScala.toMap
        Some(listStatusAsync(path.child(name), expandDirectory = false, attributes, filter))
      } else {
        None
      }
    }.toSeq

    if (childFutures.isEmpty) {
      CompletableFuture.completedFuture(Array.empty)
    } else {
      childFutures.reduce { (f1, f2) =>
        f1.thenCombine(f2, (a1: Array[FileStatus], a2: Array[FileStatus]) => a1 ++ a2)
      }
    }
  }

  private def lockTableAsync(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): CompletableFuture[FileStatus] = {
    YtWrapper.tableType(attributes) match {
      case TableType.Static => lockStaticTableAsync(path, attributes)
      case TableType.Dynamic => lockDynamicTableAsync(path, attributes)
    }
  }

  private lazy val isDriver: Boolean = SparkSession.getDefaultSession.nonEmpty

  private def lockStaticTableAsync(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): CompletableFuture[FileStatus] = {
    path.dropTimestamp().lockAsync().thenCompose(lockedPath => getFileStatus(lockedPath, attributes))
  }

  private def lockDynamicTableAsync(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): CompletableFuture[FileStatus] = {
    if (path.timestamp.isDefined || path.transaction.isDefined) {
      path.lockAsync().thenCompose(lockedPath => getFileStatus(lockedPath, attributes))
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
    getFileStatus(f, path, attributes)(yt).join()
  }

  private def getFileStatus(path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): CompletableFuture[FileStatus] = {
    getFileStatus(path.toPath, path, attributes)
  }

  private def getFileStatus(f: Path, path: YPathEnriched, attributes: Map[String, YTreeNode])
    (implicit yt: CompoundClient): CompletableFuture[FileStatus] = {
    log.debugLazy(s"Get file status $f")

    val modificationTime = YtWrapper.modificationTimeTs(attributes)
    YtWrapper.pathType(attributes) match {
      case PathType.File => buildFileStatus(f, attributes, modificationTime)

      case PathType.Table => buildTableStatus(path, attributes, modificationTime)

      case PathType.Directory => CompletableFuture.completedFuture(new FileStatus(0L, true, 1, 0L, modificationTime, f))

      case PathType.None => CompletableFuture.completedFuture(null)
    }
  }

  private def buildFileStatus(f: Path,
    attributes: Map[String, YTreeNode],
    modificationTime: Long
  ): CompletableFuture[FileStatus] = {
    val size = YtWrapper.fileSize(attributes)
    CompletableFuture.completedFuture(new FileStatus(size, false, 1, size, modificationTime, f))
  }

  private def buildTableStatus(path: YPathEnriched,
    attributes: Map[String, YTreeNode],
    modificationTime: Long
  )(implicit yt: CompoundClient): CompletableFuture[FileStatus] = {
    val size = YtWrapper.fileSize(attributes) max 1L // size may be 0 when real data exists
    val optimizeMode = YtWrapper.optimizeMode(attributes)

    val tableMetaFuture = YtWrapper.tableType(attributes) match {
      case TableType.Static =>
        buildStaticTableMeta(path, attributes, size, modificationTime, optimizeMode)

      case TableType.Dynamic =>
        buildDynamicTableMeta(attributes, size, modificationTime, optimizeMode)
    }

    tableMetaFuture.thenApply { tableMeta =>
      val richPath = YtHadoopPath(path, tableMeta)
      YtFileStatus.toFileStatus(richPath)
    }
  }

  private def buildStaticTableMeta(path: YPathEnriched,
    attributes: Map[String, YTreeNode],
    size: Long,
    modificationTime: Long,
    optimizeMode: OptimizeMode
  )(implicit yt: CompoundClient): CompletableFuture[YtTableMeta] = {
    val directRowCount = YtWrapper.rowCount(attributes)
    val resolvedRowCount = directRowCount.getOrElse(YtWrapper.chunkRowCount(attributes))

    val fullReadAllowedFuture = directRowCount match {
      case Some(_) => CompletableFuture.completedFuture(true)
      case None => isFullReadAllowed(path)
    }

    fullReadAllowedFuture.thenApply { fullReadAllowed =>
      YtTableMeta(
        rowCount = resolvedRowCount,
        size = size,
        modificationTime = modificationTime,
        optimizeMode = optimizeMode,
        fullReadAllowed = fullReadAllowed,
        schemaIdOpt = YtWrapper.schemaId(attributes)
      )
    }
  }

  private def buildDynamicTableMeta(
    attributes: Map[String, YTreeNode],
    size: Long,
    modificationTime: Long,
    optimizeMode: OptimizeMode
  ): CompletableFuture[YtTableMeta] = {
    CompletableFuture.completedFuture(YtTableMeta(
      rowCount = YtWrapper.chunkRowCount(attributes),
      size = size,
      modificationTime = modificationTime,
      optimizeMode = optimizeMode,
      isDynamic = true,
      schemaIdOpt = YtWrapper.schemaId(attributes)
    ))
  }

  private val FullReadPermission = 0x2000

  private def isFullReadAllowed(path: YPathEnriched)(implicit yt: CompoundClient): CompletableFuture[Boolean] = {
    val userFuture = Option(ytUser) match {
      case Some(user) => CompletableFuture.completedFuture(user)
      case None => yt.getCurrentUser(GetCurrentUser.builder().build())
    }

    val resultFuture = userFuture.thenCompose { user =>
      val request = CheckPermission.builder()
        .setUser(user)
        .setPath(path.toYPath.toString)
        .setPermissions(FullReadPermission)
        .setColumns(null)
        .build()

      yt.checkPermission(request)
    }

    resultFuture.thenApply { result =>
      result.getAction == ESecurityAction.SA_ALLOW
    }
  }
}

object YtTableFileSystem {
  val DEFAULT_FILTER: PathFilter = _ => true

  private class PathName(name: String) extends Path("unused") {
    override def getName: String = name
  }
}
