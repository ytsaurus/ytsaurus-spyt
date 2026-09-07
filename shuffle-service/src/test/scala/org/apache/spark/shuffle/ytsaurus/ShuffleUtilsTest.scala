package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ytsaurus.Config.YTSAURUS_SHUFFLE_CONFIG
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ShuffleUtilsTest extends AnyFlatSpec with Matchers {
  behavior of "ShuffleUtils.shuffleConfig"

  private def shuffleConfig(pushBasedEnabled: Boolean, config: String) = {
    val conf = new SparkConf(false).set(YTSAURUS_SHUFFLE_CONFIG.key, config)
    ShuffleUtils.shuffleConfig(conf, pushBasedEnabled)
  }

  it should "reject spark.ytsaurus.shuffle.config with keys other than the active shuffle mode section" in {
    an[IllegalArgumentException] should be thrownBy
      shuffleConfig(pushBasedEnabled = false, """{push={writer={codec="lz4"}}}""")
    an[IllegalArgumentException] should be thrownBy
      shuffleConfig(pushBasedEnabled = true, "{pull={writer={block_size=1024}}}")
    an[IllegalArgumentException] should be thrownBy
      shuffleConfig(pushBasedEnabled = false, "{foo={writer={block_size=1024}}}")
  }
}
