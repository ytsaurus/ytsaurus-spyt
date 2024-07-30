package tech.ytsaurus.spyt.wrapper.client

import org.slf4j.LoggerFactory

import java.time.{Duration => JDuration}
import tech.ytsaurus.spyt.wrapper.Utils
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toScalaDuration
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

import java.net.URL
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

@SerialVersionUID(6764302982752098915L)
case class YtClientConfiguration(proxy: String,
                                 user: String,
                                 token: String,
                                 timeout: Duration,
                                 proxyRole: Option[String],
                                 byop: ByopConfiguration,
                                 masterWrapperUrl: Option[String],
                                 extendedFileTimeout: Boolean,
                                 proxyNetworkName: Option[String],
                                 useCommonProxies: Boolean = false) extends Serializable {

  private def proxyUrl: Try[URL] = Try(new URL(proxy)).orElse {
    val normalizedProxy = if (proxy.contains(".") || proxy.contains(":")) {
      proxy
    } else {
      s"$proxy.yt.yandex.net"
    }
    Try(new URL(s"http://$normalizedProxy"))
  }

  def fullProxy: String = proxyUrl.map(_.getHost).get

  def port: Int = proxyUrl.map { url =>
    if (url.getPort != -1) url.getPort else if (url.getDefaultPort != -1) url.getDefaultPort else 80
  }.get

  def isHttps: Boolean = proxyUrl.toOption.exists(_.getProtocol == "https")

  def clientAuth: YTsaurusClientAuth = YTsaurusClientAuth.builder().setUser(user).setToken(token).build()

  def replaceProxy(newProxy: Option[String]): YtClientConfiguration = {
    if (newProxy.isDefined && newProxy.get != proxy) {
      copy(proxy = newProxy.get, useCommonProxies = true)
    } else {
      this
    }
  }
}

object YtClientConfiguration {
  private val log = LoggerFactory.getLogger(getClass)

  def parseEmptyWorkersListStrategy(getByName: String => Option[String]): EmptyWorkersListStrategy = {
    val strategyFromConf = for {
      name <- getByName("byop.remote.emptyWorkersList.strategy")
      timeout <- getByName("byop.remote.emptyWorkersList.timeout").map(Utils.parseDuration)
    } yield EmptyWorkersListStrategy.fromString(name, timeout)

    strategyFromConf.getOrElse(EmptyWorkersListStrategy.Default)
  }

  def apply(getByName: String => Option[String]): YtClientConfiguration = {
    val byopEnabled = getByName("byop.enabled").orElse(sys.env.get("SPARK_YT_BYOP_ENABLED")).exists(_.toBoolean)

    YtClientConfiguration(
      getByName("proxy").orElse(sys.env.get("YT_PROXY")).getOrElse(
        throw new IllegalArgumentException("Proxy must be specified")
      ),
      getByName("user").orElse(sys.env.get("YT_SECURE_VAULT_YT_USER")).getOrElse(DefaultRpcCredentials.user),
      getByName("token").orElse(sys.env.get("YT_SECURE_VAULT_YT_TOKEN")).getOrElse(DefaultRpcCredentials.token),
      getByName("timeout").map(Utils.parseDuration).getOrElse(60 seconds),
      getByName("proxyRole"),
      ByopConfiguration(
        enabled = byopEnabled,
        ByopRemoteConfiguration(
          enabled = getByName("byop.remote.enabled").map(_.toBoolean).getOrElse(byopEnabled),
          emptyWorkersListStrategy = parseEmptyWorkersListStrategy(getByName)
        )
      ),
      getByName("masterWrapper.url"),
      getByName("extendedFileTimeout").forall(_.toBoolean),
      sys.env.get("YT_JOB_ID") match {
        case Some(_) => None
        case None => getByName("proxyNetworkName")
      }
    )
  }

  def optionalApply(getByName: String => Option[String]): Option[YtClientConfiguration] = {
    val result = Try(apply(getByName))
    result match {
      case Success(value) =>
        Some(value)
      case Failure(exception) =>
        log.info("Cannot parse YTsaurus config", exception)
        None
    }
  }

  def default(proxy: String): YtClientConfiguration = default(
    proxy = proxy,
    user = DefaultRpcCredentials.user,
    token = DefaultRpcCredentials.token
  )

  def default(proxy: String, user: String, token: String): YtClientConfiguration = YtClientConfiguration(
    proxy = proxy,
    user = user,
    token = token,
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration.DISABLED,
    masterWrapperUrl = None,
    extendedFileTimeout = true,
    None
  )

  def create(proxy: String,
             user: String,
             token: String,
             timeout: JDuration,
             proxyRole: String,
             byop: ByopConfiguration,
             masterWrapperUrl: String,
             extendedFileTimeout: Boolean,
             proxyNetworkName: Option[String] = None) = new YtClientConfiguration(
    proxy, user, token, toScalaDuration(timeout),
    Option(proxyRole), byop, Option(masterWrapperUrl), extendedFileTimeout, proxyNetworkName
  )
}

