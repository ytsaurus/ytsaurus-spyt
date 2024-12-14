package spyt

import io.netty.channel.nio.NioEventLoopGroup
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.ysontree.YTree
import tech.ytsaurus.client.bus.{BusConnector, DefaultBusConnector}
import tech.ytsaurus.client.rpc.{RpcOptions, YTsaurusClientAuth}
import tech.ytsaurus.client.YTsaurusClient
import tech.ytsaurus.client.request._
import sbt.Keys._
import sbt._

import java.io.{BufferedInputStream, FileInputStream}
import java.time.Duration
import scala.annotation.tailrec
import scala.io.Source

object YtPublishPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = empty

  object autoImport {

    sealed trait YtPublishArtifact {
      def proxy: Option[String]

      def remoteDir: String

      def publish(proxyName: String, log: sbt.Logger)(implicit yt: YTsaurusClient): Unit

      def isTtlLimited: Boolean

      def forcedTTL: Option[Long]

      def ttlMillis: Option[Long] = {
        if (isTtlLimited) {
          forcedTTL.orElse(Some(Duration.ofDays(7).toMillis))
        } else {
          None
        }
      }
    }

    case class YtPublishDirectory(remoteDir: String,
                                  proxy: Option[String],
                                  override val isTtlLimited: Boolean = false,
                                  override val forcedTTL: Option[Long] = None) extends YtPublishArtifact {
      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YTsaurusClient): Unit = {
        createDir(remoteDir, proxyName, log, ttlMillis)
      }
    }

    case class YtPublishLink(originalPath: String,
                             remoteDir: String,
                             proxy: Option[String],
                             linkName: String,
                             override val isTtlLimited: Boolean = false,
                             override val forcedTTL: Option[Long] = None) extends YtPublishArtifact {
      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YTsaurusClient): Unit = {
        createDir(remoteDir, proxyName, log, ttlMillis)

        val link = s"$remoteDir/$linkName"
        log.info(s"Link $originalPath to $link..")
        yt.linkNode(
          LinkNode.builder().setSource(originalPath).setDestination(link).setIgnoreExisting(true).build()
        ).join()
      }
    }

    case class YtPublishFile(localFile: File,
                             remoteDir: String,
                             proxy: Option[String],
                             remoteName: Option[String] = None,
                             override val isTtlLimited: Boolean = false,
                             override val forcedTTL: Option[Long] = None,
                             isExecutable: Boolean = false,
                             append: Array[Byte] = Array.empty) extends YtPublishArtifact {
      private def dstName: String = remoteName.getOrElse(localFile.getName)

      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YTsaurusClient): Unit = {
        createDir(remoteDir, proxyName, log, ttlMillis)

        val src = localFile
        val dst = s"$remoteDir/$dstName"

        log.info(s"Upload $src to YT cluster $proxyName $dst..")
        val transaction = yt.startTransaction(new StartTransaction(TransactionType.Master).toBuilder
          .setTransactionTimeout(Duration.ofMinutes(10)).build()).join()
        try {
          if (yt.existsNode(dst).join()) yt.removeNode(dst).join()
          val request = CreateNode.builder()
            .setPath(YPath.simple(dst))
            .setType(CypressNodeType.FILE)
          ttlMillis.foreach(setTTL(request, _))
          yt.createNode(request.build()).join()

          val buffer = new Array[Byte](32 * 1024 * 1024)
          val writer = yt.writeFile(new WriteFile(dst).toBuilder.setTimeout(Duration.ofMinutes(10)).build()).join()
          @tailrec
          def write(len: Int): Unit = {
            writer.readyEvent().join()
            if (!writer.write(buffer, 0, len)) {
              write(len)
            }
          }

          try {
            val is = new BufferedInputStream(new FileInputStream(src))
            var writtenBytes = 0
            try {
              Stream.continually {
                val res = is.read(buffer)
                if (res > 0) write(res)
                writtenBytes += res
                res
              }.dropWhile(_ > 0)
              if (append.length > 0) {
                System.arraycopy(append, 0, buffer, 0, append.length)
                write(append.length)
              }
            } finally {
              is.close()
            }
          } finally {
            writer.close().join()
          }

          if (isExecutable) {
            yt.setNode(s"$dst/@executable", YTree.booleanNode(true)).join()
          }

          transaction.commit().join()
        } catch {
          case e: Throwable =>
            transaction.abort().join()
            throw e
        }

        log.info(s"Finished upload${ if (isExecutable) " executable"} $src to YT cluster $proxyName $dst")
      }
    }

    case class YtPublishDocument(yson: YsonableConfig,
                                 remoteDir: String,
                                 proxy: Option[String],
                                 remoteName: String,
                                 override val isTtlLimited: Boolean = false,
                                 override val forcedTTL: Option[Long] = None) extends YtPublishArtifact {
      override def publish(proxyName: String, log: sbt.Logger)(implicit yt: YTsaurusClient): Unit = {
        createDir(remoteDir, proxyName, log, ttlMillis)

        val dst = s"$remoteDir/$remoteName"
        val exists = yt.existsNode(dst).join().booleanValue()
        if (!exists) {
          log.info(s"Create document $dst at YT cluster $proxyName")
          val request = CreateNode.builder().setPath(YPath.simple(dst)).setType(CypressNodeType.DOCUMENT)
          ttlMillis.foreach(setTTL(request, _))
          yt.createNode(request.build()).join()
        }
        val ysonForPublish = yson.resolveSymlinks(yt)
        log.info(s"Upload document $ysonForPublish to YT cluster $proxyName $dst..")
        yt.setNode(dst, ysonForPublish.toYTree).join()

        log.info(s"Finished upload document to YT cluster $proxyName $dst..")
      }
    }

    def ytProxies: Seq[String] = {
      Option(System.getProperty("proxies")) match {
        case Some(value) => value.split(",").toSeq
        case None => Nil
      }
    }

    def limitTtlEnabled: Boolean = Option(System.getProperty("limitTtl")).forall(_.toBoolean)
    def configGenerationEnabled: Boolean = Option(System.getProperty("configGeneration")).forall(_.toBoolean)
    def innerSidecarConfigEnabled: Boolean = Option(System.getProperty("innerSidecarConfig")).exists(_.toBoolean)

    val publishYt = taskKey[Unit]("Publish SPYT artifacts to YTsaurus cluster")
    val publishYtArtifacts = taskKey[Seq[YtPublishArtifact]]("Artifacts to publish on the cluster")
  }

  import autoImport._

  private def setTTL(request: CreateNode.Builder, ttl: Long): Unit = {
    request.addAttribute("expiration_timeout", YTree.integerNode(ttl));
  }

  private def createDir(dir: String, proxy: String, log: Logger,
                        ttlMillis: Option[Long])(implicit yt: YTsaurusClient): Unit = {
    val exists = yt.existsNode(dir).join().booleanValue()
    if (!exists) {
      log.info(s"Create map_node $dir at YT cluster $proxy")
      val request = CreateNode.builder()
        .setPath(YPath.simple(dir))
        .setType(CypressNodeType.MAP)
        .setIgnoreExisting(true)
        .setRecursive(true)
      ttlMillis.foreach(setTTL(request, _))
      yt.createNode(request.build()).join()
    } else {
      ttlMillis.foreach { ttl =>
        log.info(s"Updating expiration timeout for map_node $dir")
        val request = new SetNode(YPath.simple(dir).attribute("expiration_timeout"), YTree.integerNode(ttl))
        yt.setNode(request).join()
      }
    }
  }

  private def createYtClient(proxy: String, credentials: YTsaurusClientAuth): (YTsaurusClient, BusConnector) = {
    val connector = new DefaultBusConnector(new NioEventLoopGroup(1), true)
      .setReadTimeout(Duration.ofMinutes(5))
      .setWriteTimeout(Duration.ofMinutes(5))

    val options = new RpcOptions()
    options.setGlobalTimeout(Duration.ofMinutes(10))
    options.setStreamingReadTimeout(Duration.ofMinutes(10))
    options.setStreamingWriteTimeout(Duration.ofMinutes(10))

    val clientBuilder = YTsaurusClient.builder()
      .setSharedBusConnector(connector)
      .setRpcOptions(options)
    if (proxy == "local") {
      val proxyHost = sys.env.getOrElse("YT_LOCAL_HOST", "localhost")
      val credentials = YTsaurusClientAuth.builder().setUser("root").setToken("").build()
      clientBuilder.setCluster(s"$proxyHost:8000").setAuth(credentials)
      clientBuilder.build() -> connector
    } else {
      clientBuilder.setCluster(proxy).setAuth(credentials)
      clientBuilder.build() -> connector
    }
  }

  private def readDefaultToken: String = {
    val f = file(sys.env("HOME")) / ".yt" / "token"
    val src = Source.fromFile(f)
    try {
      src.mkString.trim
    } finally {
      src.close()
    }
  }

  private def publishArtifact(artifact: YtPublishArtifact, proxy: String, log: Logger)
                             (implicit yt: YTsaurusClient): Unit = {
    if (artifact.proxy.forall(_ == proxy)) {
      if (sys.env.get("RELEASE_TEST").exists(_.toBoolean)) {
        log.info(s"RELEASE_TEST: Publish $artifact to $proxy")
      } else {
        artifact.publish(proxy, log)
      }
    }
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishYtArtifacts := Nil,
    publishYt := {
      val log = streams.value.log

      val creds = YTsaurusClientAuth.builder()
        .setUser(sys.env.getOrElse("YT_USER", sys.env("USER")))
        .setToken(sys.env.getOrElse("YT_TOKEN", readDefaultToken))
        .build()
      val artifacts = publishYtArtifacts.value
      val (dirs, files, links) = artifacts
        .foldLeft((Seq.empty[YtPublishArtifact], Seq.empty[YtPublishArtifact], Seq.empty[YtPublishArtifact])) {
          case ((dirs, files, links), artifact) =>
            artifact match {
              case _: YtPublishLink => (dirs, files, artifact +: links)
              case _: YtPublishDirectory => (artifact +: dirs, files, links)
              case _ => (dirs, artifact +: files, links)
            }
      }
      if (ytProxies.isEmpty) {
        log.warn("No yt proxies provided via `proxies` property.")
      }
      ytProxies.par.foreach { proxy =>
        val (ytClient, connector) = createYtClient(proxy, creds)
        implicit val yt: YTsaurusClient = ytClient
        try {
          // publish links strictly after files
          dirs.par.foreach(publishArtifact(_, proxy, log))
          files.par.foreach(publishArtifact(_, proxy, log))
          links.par.foreach(publishArtifact(_, proxy, log))
        } finally {
          yt.close()
          connector.close()
        }
      }
    }
  )
}
