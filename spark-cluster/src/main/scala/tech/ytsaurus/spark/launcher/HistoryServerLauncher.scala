package tech.ytsaurus.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.TcpProxyService
import tech.ytsaurus.spyt.wrapper.TcpProxyService.updateTcpAddress
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration

import scala.language.postfixOps

object HistoryServerLauncher extends App with VanillaLauncher with SparkLauncher {
  val log = LoggerFactory.getLogger(getClass)

  val launcherArgs = HistoryServerLauncherArgs(args)

  import launcherArgs._

  prepareProfiler()

  withYtClient(ytConfig) { yt =>
    withCypressDiscovery(baseDiscoveryPath, yt) { cypressDiscovery =>
      val tcpRouter = TcpProxyService().register("SHS")(yt)

      withService(startHistoryServer(logPath, memory, cypressDiscovery)) { historyServer =>
        val historyServerAddress =
          tcpRouter.map(_.getExternalAddress("SHS")).getOrElse(historyServer.address)

        cypressDiscovery.registerSHS(historyServerAddress)
        tcpRouter.foreach { router =>
          updateTcpAddress(historyServer.address.toString, router.getPort("SHS"))(yt)
          log.info("Tcp proxy port addresses updated")
        }

        checkPeriodically(historyServer.isAlive(3))
        log.error("Shutdown SHS")
      }
    }
  }
}

case class HistoryServerLauncherArgs(logPath: String,
                                     memory: String,
                                     ytConfig: YtClientConfiguration,
                                     baseDiscoveryPath: String)

object HistoryServerLauncherArgs {
  def apply(args: Args): HistoryServerLauncherArgs = HistoryServerLauncherArgs(
    args.required("log-path"),
    args.optional("memory").getOrElse("1G"),
    YtClientConfiguration(args.optional),
    args.optional("base-discovery-path").getOrElse(sys.env("SPARK_BASE_DISCOVERY_PATH"))
  )

  def apply(args: Array[String]): HistoryServerLauncherArgs = HistoryServerLauncherArgs(Args(args))
}
