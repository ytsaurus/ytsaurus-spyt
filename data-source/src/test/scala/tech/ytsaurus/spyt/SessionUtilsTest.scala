package tech.ytsaurus.spyt

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.SessionUtils.mergeConfs

import java.util.{Map => JMap}

class SessionUtilsTest extends AnyFlatSpec with Matchers {

  private val someConf = "spark.hadoop.yt.conf.enabled"
  private val someConf2 = "spark.hadoop.yt.conf2.enabled"

  it should "block user conf by disabled enabler" in {
    val conf = new SparkConf(false)
    conf.set(someConf, "true")
    val result = mergeConfs(conf, JMap.of(), JMap.of(), JMap.of(), JMap.of(someConf, "false", someConf2, "false"))
    result.get(someConf).toBoolean shouldBe false
    result.getOption(someConf2) shouldBe None
  }

  it should "pass user conf when enabler is enabled" in {
    val conf = new SparkConf(false)
    conf.set(someConf, "true")
    conf.set(someConf2, "true")
    val result = mergeConfs(conf, JMap.of(), JMap.of(), JMap.of(), JMap.of(someConf, "true"))
    result.get(someConf).toBoolean shouldBe true
    result.get(someConf2).toBoolean shouldBe true
  }

  it should "block cluster confs by disabled enabler" in {
    val conf = new SparkConf(false)
    val globalConf = JMap.of(someConf, "true")
    val clusterConf = JMap.of(someConf2, "true")
    val result = mergeConfs(conf, globalConf, JMap.of(), clusterConf, JMap.of(someConf, "false", someConf2, "false"))
    result.get(someConf).toBoolean shouldBe false
    result.get(someConf2).toBoolean shouldBe false
  }

  it should "prioritize user defined config" in {
    val conf = new SparkConf(false)
    conf.set(someConf, "true")
    conf.set(someConf2, "true")
    val globalConf = JMap.of(someConf, "false")
    val clusterConf = JMap.of(someConf2, "false")
    val result = mergeConfs(conf, globalConf, JMap.of(), clusterConf, JMap.of(someConf, "true", someConf2, "false"))
    result.get(someConf).toBoolean shouldBe true
    result.get(someConf2).toBoolean shouldBe false
  }
}
