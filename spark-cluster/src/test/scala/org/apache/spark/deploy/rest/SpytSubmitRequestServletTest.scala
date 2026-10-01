package org.apache.spark.deploy.rest

import org.apache.hadoop.conf.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test.LocalYt
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider, YtRpcClient}

class SpytSubmitRequestServletTest extends AnyFlatSpec with Matchers {
  behavior of "SpytSubmitRequestServlet"

  private def hadoopConf(entries: (String, String)*): Configuration = {
    val conf = new Configuration(false)
    entries.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  it should "disable the FileSystem cache for SPYT file systems" in {
    val conf = SpytSubmitRequestServlet.withSpytFsCacheDisabled(hadoopConf(
      "fs.yt.impl" -> "tech.ytsaurus.spyt.fs.YtFileSystem",
      "fs.ytCached.impl" -> "tech.ytsaurus.spyt.fs.YtCachedFileSystem"))

    conf.get("fs.yt.impl.disable.cache") shouldBe "true"
    conf.get("fs.ytCached.impl.disable.cache") shouldBe "true"
  }

  it should "keep the FileSystem cache for other file systems" in {
    val conf = SpytSubmitRequestServlet.withSpytFsCacheDisabled(hadoopConf(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem"))

    conf.get("fs.s3a.impl.disable.cache") shouldBe null
  }

  it should "ignore AbstractFileSystem implementations" in {
    val conf = SpytSubmitRequestServlet.withSpytFsCacheDisabled(hadoopConf(
      "fs.AbstractFileSystem.yt.impl" -> "tech.ytsaurus.spyt.fs.YtFs"))

    conf.get("fs.AbstractFileSystem.yt.impl.disable.cache") shouldBe null
  }

  it should "keep the other settings" in {
    val conf = SpytSubmitRequestServlet.withSpytFsCacheDisabled(hadoopConf(
      "fs.yt.impl" -> "tech.ytsaurus.spyt.fs.YtFileSystem",
      "yt.user" -> "submitter",
      "yt.token" -> "submitter-token"))

    conf.get("fs.yt.impl") shouldBe "tech.ytsaurus.spyt.fs.YtFileSystem"
    conf.get("yt.user") shouldBe "submitter"
    conf.get("yt.token") shouldBe "submitter-token"
  }

  it should "close the YT clients of the request scope when the dependency resolution fails" in {
    val ytConf = YtClientConfiguration.default(LocalYt.proxy, "root", "")
    var client: YtRpcClient = null

    val error = the [IllegalStateException] thrownBy {
      SpytSubmitRequestServlet.withClientScope { clientScope =>
        client = YtClientProvider.scopedYtRpcClient(ytConf, clientScope)
        throw new IllegalStateException("Failed to resolve the dependencies")
      }
    }

    error.getMessage shouldBe "Failed to resolve the dependencies"
    client.connector.eventLoopGroup().isShuttingDown shouldBe true
  }
}
