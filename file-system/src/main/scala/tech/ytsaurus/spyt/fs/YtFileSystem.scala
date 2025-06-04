package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.cypress.{PathType, YtAttributes}

import java.io.FileNotFoundException
import scala.language.postfixOps

@SerialVersionUID(1L)
class YtFileSystem extends YtFileSystemBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    val path = YPathEnriched.fromPath(f)
    implicit val yt: CompoundClient = ytClient(path)

    if (!YtWrapper.exists(path.toStringYPath, path.transaction)) {
      throw new PathNotFoundException(s"Path $f doesn't exist")
    }
    val pathType = YtWrapper.pathType(path.toStringYPath, path.transaction)
    pathType match {
      case PathType.File => Array(getFileStatus(f))
      case PathType.Directory => listYtDirectory(f, path.toStringYPath, path.transaction)
      case _ => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    log.debugLazy(s"Get file status $f")
    val path = YPathEnriched.fromPath(f)
    implicit val yt: CompoundClient = ytClient(path)

    if (!YtWrapper.exists(path.toStringYPath, path.transaction)) {
      throw new FileNotFoundException(s"File $f is not found")
    }
    statistics.incrementReadOps(1)
    val attributes = YtWrapper.attributes(path.toStringYPath, path.transaction,
      Set(YtAttributes.`type`, YtAttributes.compressedDataSize, YtAttributes.modificationTime))
    val pathType = YtWrapper.pathType(attributes)
    pathType match {
      case PathType.File => new FileStatus(
        YtWrapper.fileSize(attributes), false, 1, 0, YtWrapper.modificationTimeTs(attributes), f
      )
      case PathType.Directory => new FileStatus(0, true, 1, 0, 0, f)
      case PathType.None => null
      case unknown => throw new NoSuchElementException(s"Unknown PathType: '$unknown'")
    }
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    log.debugLazy(s"Create dir $f")
    val path = YPathEnriched.fromPath(f)
    implicit val yt: CompoundClient = ytClient(path)
    statistics.incrementWriteOps(1)
    YtWrapper.createDir(path.toStringYPath, ignoreExisting = true)
    true
  }
}
