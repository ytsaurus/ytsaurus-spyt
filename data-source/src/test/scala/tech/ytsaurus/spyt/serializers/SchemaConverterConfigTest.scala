package tech.ytsaurus.spyt.serializers

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.SparkVersionUtils
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.whenVariantSupported
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.YsonAsVariant
import tech.ytsaurus.spyt.serializers.SchemaConverterConfig.VARIANT_SPARK_VERSION
import tech.ytsaurus.spyt.wrapper.config.{SparkYtSparkConf, sparkConfigKeys}

class SchemaConverterConfigTest extends AnyFlatSpec with Matchers {
  behavior of "SchemaConverterConfig"

  private val ysonAsVariantConf = new SparkConf().setYtConf(YsonAsVariant, true)

  it should "read ysonAsVariant on Spark versions with the Variant type" in {
    whenVariantSupported {
      SchemaConverterConfig(ysonAsVariantConf).ysonAsVariant shouldBe true
    }
  }

  it should "reject ysonAsVariant on Spark versions without the Variant type" in {
    assume(SparkVersionUtils.lessThan(VARIANT_SPARK_VERSION), s"Spark ${SparkVersionUtils.currentVersion} has Variant")
    val error = the[UnsupportedOperationException] thrownBy SchemaConverterConfig(ysonAsVariantConf)
    error.getMessage should include(sparkConfigKeys(YsonAsVariant))
  }
}
