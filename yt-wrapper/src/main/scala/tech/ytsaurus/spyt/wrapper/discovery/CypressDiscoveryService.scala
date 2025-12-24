package tech.ytsaurus.spyt.wrapper.discovery

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.operation.OperationStatus
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.GetOperation
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.YtWrapper.readDocument
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import java.net.URI
import java.util.Optional
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success, Try}

class CypressDiscoveryService(baseDiscoveryPath: String)(implicit yt: CompoundClient) extends DiscoveryService {
  private val log = LoggerFactory.getLogger(getClass)

  private val discoveryPath: String = s"$baseDiscoveryPath/discovery"

  private val addressPath: String = s"$discoveryPath/spark_address"

  private val webUiPath: String = s"$discoveryPath/webui"

  private val masterJobsPath: String = s"$discoveryPath/master_jobs"

  private val webUiUrlAttribute: String = "webui_url"

  private val restPath: String = s"$discoveryPath/rest"

  private val operationPath: String = s"$discoveryPath/operation"

  private val childrenOperationsPath: String = s"$discoveryPath/children_operations"

  private val shsPath: String = s"$discoveryPath/shs"

  private val livyPath: String = s"$discoveryPath/livy"

  private val clusterVersionPath: String = s"$discoveryPath/version"

  private val confPath: String = s"$discoveryPath/conf"

  private val masterWrapperPath: String = s"$discoveryPath/master_wrapper"

  override def operationInfo: Option[OperationInfo] = operation.flatMap(oid => {
    val id = GUID.valueOf(oid)
    val r = yt.getOperation(new GetOperation(id)).join()
    if (r.getAttribute("state").isPresent) {
      Try(OperationStatus.getByName(r.getAttribute("state").get().stringValue()))
        .toOption
        .map(s => OperationInfo(id, s))
    } else None
  })

  override def registerMaster(operationId: String,
                              address: Address,
                              clusterVersion: String,
                              masterWrapperEndpoint: HostAndPort,
                              clusterConf: SparkConfYsonable): Unit = {
    val clearDir = getPath(operationPath) match {
      case Success(_) if operation.exists(_ != operationId) && operationInfo.exists(!_.state.isFinished) =>
        throw new IllegalStateException(s"Spark instance with path $discoveryPath already exists")
      case Success(_) =>
        log.info(s"Spark instance with path $discoveryPath registered, but is not alive, rewriting id")
        true
      case Failure(EmptyDirectoryException(_)) =>
        log.info(s"Spark instance with path $discoveryPath doesn't exist, registering new one")
        false
      case Failure(ex) =>
        throw ex
    }

    val transaction = YtWrapper.createTransaction(None, 1 minute)
    val tr = Some(transaction.getId.toString)
    try {
      if (clearDir) removeAddress(tr)
      YtWrapper.createDir(s"$addressPath/${YtWrapper.escape(address.hostAndPort.toString)}", tr)
      Map(
        webUiPath -> YtWrapper.escape(address.webUiHostAndPort.toString),
        restPath -> YtWrapper.escape(address.restHostAndPort.toString),
        operationPath -> operationId,
        clusterVersionPath -> clusterVersion,
        masterWrapperPath -> YtWrapper.escape(masterWrapperEndpoint.toString)
      ).foreach { case (path, value) =>
        YtWrapper.createDir(s"$path/$value", tr)
      }
      YtWrapper.createDocument(s"$masterJobsPath/${System.getenv("YT_JOB_ID")}", YTree.mapBuilder()
        .key(webUiUrlAttribute).value(address.webUiUri.toString)
        .endMap().build(), tr, recursive = true)
      YtWrapper.createDocumentFromProduct(confPath, clusterConf, tr)
    } catch {
      case e: Throwable =>
        transaction.abort().join()
        throw e
    }
    transaction.commit().join()
  }

  override def registerWorker(operationId: String): Unit = {
    log.info(s"Registering worker operation $operationId")
    if (!operation.contains(operationId) && operationId != null && !operationId.isBlank) {
      log.info(s"Registering worker operation $operationId: started")
      val tr = YtWrapper.createTransaction(None, 1 minute)
      YtWrapper.createDir(s"$childrenOperationsPath/$operationId", Some(tr.getId.toString), ignoreExisting = true)
      tr.commit().join()
      log.info(s"Registering worker operation $operationId: completed")
    }
  }

  private def registerSimpleService(dirPath: String, address: HostAndPort): Unit = {
    val transaction = YtWrapper.createTransaction(None, 1 minute)
    val tr = Some(transaction.getId.toString)
    val addr = YtWrapper.escape(address.toString)
    YtWrapper.removeDir(dirPath, recursive = true, force = true, transaction = tr)
    YtWrapper.createDir(s"$dirPath/$addr", tr)
    transaction.commit().join()
  }

  override def registerSHS(address: HostAndPort): Unit = {
    registerSimpleService(shsPath, address)
  }

  override def registerLivy(address: HostAndPort, livyVersion: String): Unit = {
    registerSimpleService(livyPath, address)
  }

  private def cypressHostAndPort(path: String): Try[HostAndPort] = {
    getPath(path).map(HostAndPort.fromString)
  }

  private def getPath(path: String): Try[String] =
    if (YtWrapper.exists(path))
      Try(YtWrapper.listDir(path)).map(_.head)
    else
      Failure(EmptyDirectoryException(s"Path not found: $path"))


  override def discoverAddress(): Try[Address] =
    for {
      _ <- getPath(confPath).recover { case InvalidCatalogException(msg) => EmptyDirectoryException(msg) }
      hostAndPort <- cypressHostAndPort(addressPath)
      webUiHostAndPort <- cypressHostAndPort(webUiPath)
      webUiUrl <- getWebUiUrl()
      restHostAndPort <- cypressHostAndPort(restPath)
    } yield Address(hostAndPort, webUiHostAndPort, webUiUrl, restHostAndPort)

  def clusterVersion: Try[String] = getPath(clusterVersionPath)

  private def getWebUiUrl(): Try[URI] = getPath(masterJobsPath).map(jobId =>
    URI.create(readDocument(s"$masterJobsPath/$jobId").mapNode().getOrThrow(webUiUrlAttribute).stringValue())
  )

  override def masterWrapperEndpoint(): Option[HostAndPort] = cypressHostAndPort(masterWrapperPath).toOption

  private def operation: Option[String] = getPath(operationPath).toOption

  override def operations(): Option[OperationSet] = {
    def isDriverOp: String => Boolean = opId => {
      import CypressDiscoveryService._
      yt.getOperation(new GetOperation(GUID.valueOf(opId)))
        .join()
        .path("full_spec", "tasks", "drivers")
        .isDefined
    }
    operation.map(masterId => {
      val allChildren = if (YtWrapper.exists(childrenOperationsPath)) {
        YtWrapper.listDir(childrenOperationsPath).toSet
      } else {
        Set[String]()
      }
      val children = allChildren.filterNot(isDriverOp)
      val driverOp = allChildren.find(isDriverOp)
      OperationSet(masterId, children, driverOp)
    })
  }

  override def toString: String = s"CypressDiscovery[$baseDiscoveryPath]"

  private def removeAddress(transaction: Option[String]): Unit = {
    YtWrapper.listDir(discoveryPath)
      .map(name => s"$discoveryPath/$name")
      .filter(_ != shsPath)
      .foreach(YtWrapper.removeDir(_, recursive = true, force = true, transaction = transaction))
  }
}

object CypressDiscoveryService {
  def eventLogPath(discoveryBasePath: String): String = {
    s"$discoveryBasePath/logs/event_log_table"
  }

  implicit def convertOptional[T](opt: Optional[T]): Option[T] = {
    if (opt.isPresent)
      Some(opt.get())
    else
      None
  }

  implicit class YTreeNodeExt(n: YTreeNode) {
    def path(path: String*): Option[YTreeNode] =
      path.foldLeft(Option(n)) { case (no, p) =>
        no.flatMap(v => Option(v.asMap().get(p)))
      }

    def longAttribute(path: String*): Option[Long] = n.path(path:_*).map(_.longValue())
  }
}

case class InvalidCatalogException(message: String) extends RuntimeException(message)
case class EmptyDirectoryException(message: String) extends RuntimeException(message)

