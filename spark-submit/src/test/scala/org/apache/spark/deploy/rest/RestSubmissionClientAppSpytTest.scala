package org.apache.spark.deploy.rest

import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RestSubmissionClientAppSpytTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  behavior of "RestSubmissionClientAppSpyt"

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

  it should "map spark.yt confs to UPPERCASE underscore env names and filter the rest" in {
    val conf = new SparkConf(false)
      .set("spark.yt.read.ytPartitioning.enabled", "true")
      .set("spark.hadoop.yt.proxy", "host")
      .set("spark.app.name", "ignored")
    val env = RestSubmissionClientAppSpyt.buildConfEnv(conf)
    env should contain ("SPARK_YT_READ_YTPARTITIONING_ENABLED" -> "true")
    env should contain ("SPARK_HADOOP_YT_PROXY" -> "host")
    env.keys.exists(_.contains("APP_NAME")) shouldBe false
  }

  it should "not produce a case-divergent duplicate after a producer to driver round-trip" in {
    val submitConf = new SparkConf(false)
      .set("spark.yt.read.ytPartitioning.enabled", "true")
      .set("spark.job.args.read_from_env", "true")

    val confEnv = RestSubmissionClientAppSpyt.buildConfEnv(submitConf)
    confEnv.keySet should contain ("SPARK_YT_READ_YTPARTITIONING_ENABLED")
    confEnv.keySet should not contain "SPARK_JOB_ARGS_READ_FROM_ENV"

    System.setProperty("spark.yt.read.ytPartitioning.enabled", "true")
    System.setProperty("spark.job.args.read_from_env", "true")
    confEnv.foreach { case (k, v) => setEnvVariable(k, v) }
    setEnvVariable("SPARK_JOB_ARGS_READ_FROM_ENV", "true")
    val driverConf = new SparkConf()

    driverConf.getAll.count { case (k, _) => k.equalsIgnoreCase("spark.yt.read.ytPartitioning.enabled") } shouldBe 1
    driverConf.getOption("spark.yt.read.ytPartitioning.enabled") shouldBe Some("true")
    driverConf.getOption("spark.yt.read.ytpartitioning.enabled") shouldBe None

    driverConf.getAll.count { case (k, _) => k.equalsIgnoreCase("spark.job.args.read_from_env") } shouldBe 1
    driverConf.getOption("spark.job.args.read_from_env") shouldBe Some("true")
    driverConf.getOption("spark.job.args.read.from.env") shouldBe None
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
