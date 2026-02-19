package org.apache.spark.deploy

import org.apache.spark.SparkException
import org.apache.spark.internal.config.{CORES_MAX, DECOMMISSION_ENABLED, DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, EXECUTOR_INSTANCES, FILES, SUBMIT_PYTHON_FILES}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.Seq

class SparkSubmitSpytTest extends AnyFlatSpec with Matchers {
  behavior of "SparkSubmit"

  private val submit = new SparkSubmit()

  it should "submit applications to YTsaurus scheduler in client mode" in {
    val appArgs = new SparkSubmitArguments(ytArgs("client"))

    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)

    mainClass shouldBe "org.apache.spark.deploy.PythonRunner"
    conf.get(EXECUTOR_INSTANCES) shouldBe Some(2)
    conf.get("spark.hadoop.yt.proxy") shouldBe "my.yt.cluster"
    conf.get("spark.ytsaurus.pool") shouldBe "research"
    conf.get(FILES) should contain theSameElementsInOrderAs Seq("yt:/path/to/my/super/lib.zip")
    childArgs.head should endWith ("app.py")
    childArgs.tail.head should endWith ("lib.zip")
    childArgs.tail.tail should contain theSameElementsAs Seq("some", "--weird", "args")
  }

  it should "submit applications to YTsaurus scheduler in cluster mode" in {
    val appArgs = new SparkSubmitArguments(ytArgs("cluster"))
    appArgs.pyFiles shouldBe "yt:///path/to/my/super/lib.zip"

    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)

    mainClass shouldBe "org.apache.spark.deploy.ytsaurus.YTsaurusClusterApplication"
    conf.get(EXECUTOR_INSTANCES) shouldBe Some(2)
    conf.get("spark.hadoop.yt.proxy") shouldBe "my.yt.cluster"
    conf.get("spark.ytsaurus.pool") shouldBe "research"
    conf.get(FILES) shouldBe empty
    conf.get(SUBMIT_PYTHON_FILES) should contain theSameElementsInOrderAs Seq("yt:/path/to/my/super/lib.zip")
    childArgs should contain theSameElementsAs Seq(
      "--primary-py-file", "yt:///path/to/my/super/app.py",
      "--main-class", "org.apache.spark.deploy.PythonRunner",
      "--arg", "some", "--arg", "--weird", "--arg", "args"
    )
  }

  it should "submit python applications to Spark Standalone cluster in cluster mode" in {
    val appArgs = new SparkSubmitArguments(standaloneArgs)

    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)

    mainClass shouldBe "org.apache.spark.deploy.rest.RestSubmissionClientApp"
    conf.get(CORES_MAX) shouldBe Some(20)
    conf.get(FILES) should contain theSameElementsInOrderAs Seq("yt:/path/to/my/super/lib.zip")
    conf.get(SUBMIT_PYTHON_FILES) should contain theSameElementsInOrderAs Seq("yt:/path/to/my/super/lib.zip")
    childArgs should contain theSameElementsAs Seq(
      "yt:///path/to/my/super/app.py",
      "org.apache.spark.deploy.PythonRunner",
      "{{USER_JAR}}", "{{PY_FILES}}",
      "some", "--weird", "args"
    )
  }

  it should "detect the resource as binary" in {
    SparkSubmitSpyt.isBinary("yt:///path/to/my/script.py") shouldBe false
    SparkSubmitSpyt.isBinary("script.py") shouldBe false
    SparkSubmitSpyt.isBinary("yt:///path/to/my/application.jar") shouldBe false
    SparkSubmitSpyt.isBinary("yt:///path/to/my/application.bin") shouldBe true
    SparkSubmitSpyt.isBinary("//path/to/my/executable") shouldBe true
    SparkSubmitSpyt.isBinary("strange_file.") shouldBe true
    SparkSubmitSpyt.isBinary("spark-shell") shouldBe false
    SparkSubmitSpyt.isBinary("spark-internal") shouldBe false
  }

  it should "fail dynamic allocation when YTsaurus shuffle service is disabled" in {
    val appArgs = new SparkSubmitArguments(dynamicAllocationArgs(false))
    val ex = intercept[SparkException] {
      submit.prepareSubmitEnvironment(appArgs)
    }
    ex.getMessage should include ("Dynamic allocation requires YTsaurus shuffle service.")
  }

  it should "fail dynamic allocation when executor instances are not specified" in {
    val appArgs = new SparkSubmitArguments(dynamicAllocationArgs(true, 0))
    val ex = intercept[SparkException] {
      submit.prepareSubmitEnvironment(appArgs)
    }
    ex.getMessage should include ("Number of executors must be a positive number.")
  }

  it should "set proper parameters when dynamic allocation is enabled" in {
    val appArgs = new SparkSubmitArguments(dynamicAllocationArgs(true))

    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)

    conf.get(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED) shouldBe false
    conf.get(DECOMMISSION_ENABLED) shouldBe true
  }

  private def ytArgs(deployMode: String): Seq[String] = Seq(
    "--master", "ytsaurus://my.yt.cluster",
    "--deploy-mode", deployMode,
    "--num-executors", "2",
    "--queue", "research",
    "--py-files", "yt:///path/to/my/super/lib.zip",
    "--conf", "spark.hadoop.fs.yt.impl=tech.ytsaurus.spyt.fs.MockYtFileSystem",
    "yt:///path/to/my/super/app.py",
    "some", "--weird", "args"
  )

  private val standaloneArgs = Seq(
    "--master", "spark://master.address",
    "--deploy-mode", "cluster",
    "--total-executor-cores", "20",
    "--py-files", "yt:///path/to/my/super/lib.zip",
    "--conf", "spark.hadoop.fs.yt.impl=tech.ytsaurus.spyt.fs.MockYtFileSystem",
    "--conf", "spark.master.rest.enabled=true",
    "yt:///path/to/my/super/app.py",
    "some", "--weird", "args"
  )

  private def dynamicAllocationArgs(ytsaurusShuffleEnabled: Boolean, initialExecutors: Int = 2): Seq[String] = Seq(
    "--master", "ytsaurus://my.yt.cluster",
    "--deploy-mode", "cluster",
    "--queue", "research",
    "--py-files", "yt:///path/to/my/super/lib.zip",
    "--conf", "spark.hadoop.fs.yt.impl=tech.ytsaurus.spyt.fs.MockYtFileSystem",
    "--conf", "spark.dynamicAllocation.enabled=true",
    "--conf", f"spark.dynamicAllocation.initialExecutors=${initialExecutors.toString}",
    "--conf", f"spark.ytsaurus.shuffle.enabled=${ytsaurusShuffleEnabled.toString}",
    "yt:///path/to/my/super/app.py"
  )

}
