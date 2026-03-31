package tech.ytsaurus.spyt.wrapper.client

import org.slf4j.LoggerFactory
import tech.ytsaurus.client.rpc.YTsaurusClientAuth
import tech.ytsaurus.spyt.wrapper.Utils

import java.net.URL
import java.time.Duration

import scala.util.{Failure, Success, Try}

@SerialVersionUID(-4051409700916829022L)
case class YtClientConfiguration(
  proxy: String,
  user: String,
  token: String,
  timeout: Duration,
  proxyRole: Option[String],
  extendedFileTimeout: Boolean,
  proxyNetworkName: Option[String],
  useCommonProxies: Boolean = false) extends Serializable {

  private lazy val initialUrlAttempt: Try[URL] = Try(new URL(proxy))

  def normalizedProxy: String = initialUrlAttempt match {
    case Success(_) => proxy
    case Failure(_) =>
      if (proxy.contains(".") || proxy.contains(":")) proxy
      else s"$proxy.yt.yandex.net"
  }

  private def proxyUrl: Try[URL] = initialUrlAttempt.orElse {
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

  def apply(getByName: String => Option[String]): YtClientConfiguration = {
    val token = getByName("token").orElse(sys.env.get("YT_SECURE_VAULT_YT_TOKEN"))
      .getOrElse(DefaultRpcCredentials.token)
    val proxy = getByName("proxy").orElse(sys.env.get("YT_PROXY")).getOrElse(
      throw new IllegalArgumentException("Proxy must be specified")
    )
    val user = getByName("user").orElse(sys.env.get("YT_SECURE_VAULT_YT_USER"))
      .getOrElse(DefaultRpcCredentials.tokenUser(token))
    val timeout = getByName("timeout").map(Utils.parseDuration).getOrElse(Duration.ofSeconds(60))
    val extendedFileTimeout = getByName("extendedFileTimeout").forall(_.toBoolean)
    val proxyNetworkName = sys.env.get("YT_JOB_ID") match {
      case Some(_) => None
      case None => getByName("proxyNetworkName")
    }

    YtClientConfiguration(proxy, user, token, timeout, getByName("proxyRole"),
      extendedFileTimeout, proxyNetworkName)
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

  def default(proxy: String): YtClientConfiguration = {
    val token = DefaultRpcCredentials.token
    default(
      proxy = proxy,
      user = DefaultRpcCredentials.tokenUser(token),
      token = token,
    )
  }

  def default(proxy: String, user: String, token: String): YtClientConfiguration = YtClientConfiguration(
    proxy = proxy,
    user = user,
    token = token,
    timeout = Duration.ofMinutes(5),
    proxyRole = None,
    extendedFileTimeout = true,
    None
  )

  def create(
    proxy: String,
    user: String,
    token: String,
    timeout: Duration,
    proxyRole: String,
    extendedFileTimeout: Boolean,
    proxyNetworkName: Option[String] = None): YtClientConfiguration = {
    new YtClientConfiguration(
      proxy, user, token, timeout,Option(proxyRole), extendedFileTimeout, proxyNetworkName)
  }
}

