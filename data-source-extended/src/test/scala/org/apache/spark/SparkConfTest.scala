package org.apache.spark

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * This test was written because SparkConfDecorators used @Subclass annotation which lead to changing signature of all
 * builder pattern methods, i.e. that were returning SparkConf instances. So other classes couldn't find these
 * methods by their original signature. So, @Subclass was changed to @Decarate and this test ensures that everything
 * works as expected.
 */
class SparkConfTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  behavior of "SparkConf"

  private var savedEnv: java.util.Map[String, String] = _
  private var savedProperties: java.util.Properties = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    savedEnv = new java.util.HashMap[String, String](System.getenv())
    savedProperties = System.getProperties().clone().asInstanceOf[java.util.Properties]
  }

  override def afterEach(): Unit = {
    try {
      val innerEnvMap = mutableEnv()
      innerEnvMap.clear()
      innerEnvMap.putAll(savedEnv)
      System.setProperties(savedProperties)
    } finally {
      super.afterEach()
    }
  }

  it should "set a missing attribute" in {
    val conf = new SparkConf()
    conf.getOption("some.key") shouldBe None

    conf.setIfMissing("some.key", "some.value")
    conf.getOption("some.key") shouldBe Some("some.value")

    conf.setIfMissing("some.key", "updated.value")
    conf.getOption("some.key") shouldBe Some("some.value")
  }

  it should "set spark properties from environment" in {
    setEnvVariable("SPARK_YT_VERSION", "value")
    val conf = new SparkConf()
    conf.getOption("spark.yt.version") shouldBe Some("value")
  }

  it should "set spark properties from secure vault" in {
    setEnvVariable("YT_SECURE_VAULT_SPARK_SOME_SECRET_KEY", "aKeyToHide")
    val conf = new SparkConf()
    conf.getOption("spark.some.secret.key") shouldBe Some("aKeyToHide")
  }

  it should "not create a lower-cased duplicate of an explicitly set camelCase conf provided via environment" in {
    System.setProperty("spark.yt.read.ytPartitioning.enabled", "false")
    setEnvVariable("SPARK_YT_READ_YTPARTITIONING_ENABLED", "true")
    val conf = new SparkConf()
    conf.getAll.count { case (k, _) => k.equalsIgnoreCase("spark.yt.read.ytPartitioning.enabled") } shouldBe 1
    conf.getOption("spark.yt.read.ytPartitioning.enabled") shouldBe Some("false")
    conf.getOption("spark.yt.read.ytpartitioning.enabled") shouldBe None

  }

  it should "not let a secure vault secret override an explicitly set conf" in {
    System.setProperty("spark.yt.token", "explicit-token")
    setEnvVariable("YT_SECURE_VAULT_SPARK_YT_TOKEN", "vault-token")
    val conf = new SparkConf()
    conf.getOption("spark.yt.token") shouldBe Some("explicit-token")
    conf.getAll.count { case (k, _) => k.equalsIgnoreCase("spark.yt.token") } shouldBe 1
  }

  it should "not let an environment variable override an explicitly set conf" in {
    System.setProperty("spark.yt.exactoverridecheck.enabled", "false")
    setEnvVariable("SPARK_YT_EXACTOVERRIDECHECK_ENABLED", "true")
    val conf = new SparkConf()
    conf.getOption("spark.yt.exactoverridecheck.enabled") shouldBe Some("false")
    conf.getAll.count { case (k, _) => k.equalsIgnoreCase("spark.yt.exactoverridecheck.enabled") } shouldBe 1
  }

  it should "not create a dotted duplicate of an underscore conf provided via environment" in {
    System.setProperty("spark.job.args.read_from_env", "v1")
    setEnvVariable("SPARK_JOB_ARGS_READ_FROM_ENV", "v2")
    val conf = new SparkConf()
    conf.getOption("spark.job.args.read_from_env") shouldBe Some("v1")
    conf.getOption("spark.job.args.read.from.env") shouldBe None
  }

  private def setEnvVariable(name: String, value: String): Unit = {
    mutableEnv().put(name, value)
  }

  private def mutableEnv(): java.util.Map[String, String] = {
    val envMap = System.getenv()
    val field = envMap.getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(envMap).asInstanceOf[java.util.Map[String, String]]
  }
}
