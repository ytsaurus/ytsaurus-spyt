package tech.ytsaurus.spark.launcher.rest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YtOutputFormatTest extends AnyFlatSpec with Matchers {

  behavior of "YtOutputFormatTest"

  it should "format discovery info to json" in {
    val info = DiscoveryInfo(Seq("a:1", "b:2"))
    YtOutputFormat.Json.format(info) shouldEqual """{"proxies":["a:1","b:2"]}""".stripMargin
  }

  it should "format discovery info to yson" in {
    val info = DiscoveryInfo(Seq("a:1", "b:2"))
    YtOutputFormat.Yson.format(info) shouldEqual """{"proxies"=["a:1";"b:2";];}"""
  }
}
