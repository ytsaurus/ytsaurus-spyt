package tech.ytsaurus.spyt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.slf4j.LoggerFactory
import tech.ytsaurus.TError
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.common.YTsaurusError
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter._
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider, YtRpcClient}
import tech.ytsaurus.spyt.wrapper.{LogLazy, YtWrapper}

import java.io.FileNotFoundException
import java.net.URI
import java.util.concurrent.CompletionException
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.duration._
import scala.language.postfixOps

abstract class YtFileSystemBase extends FileSystem with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)

  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")

  private var defaultYtConf: YtClientConfiguration = _

  protected[fs] def ytClient(f: YPathEnriched): CompoundClient = {
    YtClientProvider.ytClientWithProxy(defaultYtConf, f.cluster)
  }

  private[fs] def validateSameCluster(src: YPathEnriched, dst: YPathEnriched): Unit = {
    if (src.cluster != dst.cluster) {
      throw new IllegalArgumentException(
        s"Source (${src.cluster}) and destination (${dst.cluster}) must be located on same cluster")
    }
  }

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    setConf(conf)
    this._uri = uri
    this.defaultYtConf = ytClientConfiguration(getConf)
  }

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = convertExceptions {
    log.debugLazy(s"Open file ${f.toUri.toString}")
    statistics.incrementReadOps(1)
    val path = YPathEnriched.fromPath(f)
    val inputStream = YtWrapper.readFile(path.toYPath, path.transaction, timeout = defaultYtConf.timeout)(ytClient(path))
    new FSDataInputStream(new YtFsInputStream(inputStream, statistics))
  }

  protected def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                       replication: Short, blockSize: Long, progress: Progressable,
                       statistics: FileSystem.Statistics): FSDataOutputStream = convertExceptions {
    log.debugLazy(s"Create new file: $f")
    val path = YPathEnriched.fromPath(f)
    implicit val yt: CompoundClient = ytClient(path)

    statistics.incrementWriteOps(1)
    YtWrapper.createDir(path.toYPath.parent(), path.transaction, ignoreExisting = true)(yt)

    def createFile(ytRpcClient: Option[YtRpcClient], ytClient: CompoundClient): FSDataOutputStream = {
      YtWrapper.createFile(path.toYPath, path.transaction, overwrite, recursive = false)(ytClient)
      statistics.incrementWriteOps(1)
      val writeFile = YtWrapper.writeFile(path.toYPath, 7 days, ytRpcClient, path.transaction)(ytClient)
      new FSDataOutputStream(writeFile, statistics)
    }

    if (defaultYtConf.extendedFileTimeout) {
      val ytConf = defaultYtConf.copy(timeout = 7 days)
      val ytRpcClient: YtRpcClient = YtClientProvider.ytRpcClient(ytConf)
      try {
        createFile(Some(ytRpcClient), ytRpcClient.yt)
      } catch {
        case e: Throwable =>
          throw e
      }
    } else {
      createFile(None, yt)
    }
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    create(f, permission, overwrite, bufferSize, replication, blockSize, progress, statistics)
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = convertExceptions {
    ???
  }

  override def rename(src: Path, dst: Path): Boolean = convertExceptions {
    log.debugLazy(s"Rename $src to $dst")
    val srcPath = YPathEnriched.fromPath(src)
    val dstPath = YPathEnriched.fromPath(dst)
    validateSameCluster(srcPath, dstPath)
    statistics.incrementWriteOps(1)
    YtWrapper.move(srcPath.toStringYPath, dstPath.toStringYPath)(ytClient(srcPath))
    true
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = convertExceptions {
    log.debugLazy(s"Create $f")
    val path = YPathEnriched.fromPath(f)
    statistics.incrementWriteOps(1)
    YtWrapper.createDir(path.toStringYPath, ignoreExisting = true)(ytClient(path))
    true
  }

  override def delete(f: Path, recursive: Boolean): Boolean = convertExceptions {
    log.debugLazy(s"Delete $f")
    val path = YPathEnriched.fromPath(f)
    implicit val yt: CompoundClient = ytClient(path)
    if (!YtWrapper.exists(path.toStringYPath, path.transaction)) {
      log.debugLazy(s"$f is not exist")
      false
    } else {
      statistics.incrementWriteOps(1)
      YtWrapper.remove(path.toStringYPath, path.transaction)
      true
    }
  }

  def listYtDirectory(f: Path, path: String, transaction: Option[String])
                     (implicit yt: CompoundClient): Array[FileStatus] = convertExceptions {
    statistics.incrementReadOps(1)
    YtWrapper.listDir(path, transaction).map(name => getFileStatus(new Path(f, name)))
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  override def close(): Unit = {
    super.close()
  }

  def internalStatistics: FileSystem.Statistics = this.statistics

  def convertExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case FileNotFound(ex) => throw ex
    }
  }


  private object FileNotFound {
    private val ERR_PATH_PFX = "Error resolving path "
    private val ERR_CODE = 500

    def findFileNotFound(e: TError): Option[String] = {
      if (e.getCode == ERR_CODE && e.getMessage.startsWith(ERR_PATH_PFX)) {
        Some(e.getMessage.substring(ERR_PATH_PFX.length))
      } else if (e.getInnerErrorsList.isEmpty) {
        None
      } else {
        e.getInnerErrorsList.toSeq.flatMap(findFileNotFound).headOption
      }
    }

    @tailrec
    def unapply(ex: Throwable): Option[FileNotFoundException] = ex match {
      case err: CompletionException if err.getCause != null =>
        unapply(err.getCause)
      case err: YTsaurusError =>
        findFileNotFound(err.getError).foreach(file => {
          throw new FileNotFoundException(file) {
            override def getCause: Throwable = err
          }
        })
        throw err
      case unknown: Throwable =>
        throw unknown
    }
  }
}
