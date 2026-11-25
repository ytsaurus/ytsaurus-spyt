package tech.ytsaurus.spyt.wrapper.client

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.request.GetCurrentUser
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Using

class YtClientConfigurationConverterTest extends AnyFlatSpec with Matchers {
  behavior of "YtClientConfigurationConverter"

  private val ytSettings = Seq(
    "spark.hadoop.yt.proxy" -> "test-proxy",
    "spark.hadoop.yt.user" -> "test-user",
    "spark.hadoop.yt.token" -> "test-token",
    "spark.hadoop.yt.timeout" -> "5m"
  )

  private def sparkConfWithYtSettings: SparkConf = {
    val conf = new SparkConf(false)
    ytSettings.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  private def checkYtSettings(conf: YtClientConfiguration): Unit = {
    conf.proxy shouldEqual "test-proxy"
    conf.user shouldEqual "test-user"
    conf.token shouldEqual "test-token"
    conf.timeout shouldEqual Duration.ofMinutes(5)
  }

  it should "read yt settings from spark conf" in {
    checkYtSettings(ytClientConfiguration(sparkConfWithYtSettings))
  }

  it should "tunnel fixed-proxy RPC through the HTTP proxy configured in Spark" in {
    Using.Manager { use =>
      val listener = use(new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")))
      listener.setSoTimeout(10000)
      val proxy = s"127.0.0.1:${listener.getLocalPort}"
      val conf = ytClientConfiguration(sparkConfWithYtSettings.set("spark.hadoop.yt.httpConnectProxy", proxy))
      val target = s"127.0.0.2:${listener.getLocalPort}"
      val client = use(YtWrapper.createRpcClient(conf.copy(fixedProxyAddress = Some(target)), 1))
      client.yt.getCurrentUser(GetCurrentUser.builder().build())

      val socket = use(listener.accept())
      socket.setSoTimeout(10000)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.US_ASCII))
      reader.readLine() shouldEqual s"CONNECT $target HTTP/1.1"
    }.get
  }

  it should "read yt settings from sql conf" in {
    val conf = new SQLConf()
    ytSettings.foreach { case (key, value) => conf.setConfString(key, value) }

    checkYtSettings(ytClientConfiguration(conf))
  }

  it should "take yt settings only from the spark.hadoop prefix" in {
    val conf = sparkConfWithYtSettings
    conf.set("yt.proxy", "unused-proxy")
    conf.set("spark.yt.proxy", "unused-proxy")

    ytClientConfiguration(conf).proxy shouldEqual "test-proxy"
  }

  it should "not load hadoop default resources" in {
    val result = YtClientConfigurationConverter.hadoopConf(sparkConfWithYtSettings.getAll)

    result.iterator().asScala.map(_.getKey).toSeq should contain theSameElementsAs
      ytSettings.map { case (key, _) => key.stripPrefix("spark.hadoop.") }
  }
}
