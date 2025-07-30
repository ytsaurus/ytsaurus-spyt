
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.deploy.ytsaurus.ApplicationArguments
import org.apache.spark.deploy.ytsaurus.Config._
import org.apache.spark.internal.config.{ARCHIVES, DRIVER_HOST_ADDRESS, DRIVER_PORT, FILES, JARS, SUBMIT_PYTHON_FILES}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager.{ApplicationFile, DRIVER_TASK, EXECUTOR_TASK}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.YTsaurusClient
import tech.ytsaurus.client.operations.VanillaSpec
import tech.ytsaurus.spyt.wrapper.file.YtFileUtils
import tech.ytsaurus.ysontree._

import scala.collection.JavaConverters._

class YTsaurusOperationManagerSuite extends SparkFunSuite with BeforeAndAfter with Matchers {

  private val testSparkConf: SparkConf = {
    new SparkConf()
      .set(DRIVER_HOST_ADDRESS, "some-host")
      .set(DRIVER_PORT, 12345)
  }

  private val testResourceProfile: ResourceProfile = ResourceProfile.getOrCreateDefaultProfile(testSparkConf)

  private def createYTsaurusOperationManagerStub(
    ytClient: YTsaurusClient = null,
    user: String = "testUser",
    token: String = "testToken",
    layerPaths: YTreeNode = YTree.listBuilder().buildList(),
    filePaths: YTreeNode = YTree.listBuilder().buildList(),
    environment: YTreeMapNode = YTree.mapBuilder().buildMap(),
    home: String = ".",
    prepareEnvCommand: String = "./setup-spyt-env.sh --some-key some-value",
    sparkClassPath: String = "./*:/usr/lib/spyt/conf/:/usr/lib/spyt/jars/*:/usr/lib/spark/jars/*",
    javaCommand: String = "/usr/bin/java",
    ytsaurusJavaOptions: Seq[String] = Seq.empty[String]
  ): YTsaurusOperationManager = {
    new YTsaurusOperationManager(ytClient, user, token, layerPaths, filePaths, environment, home,
      prepareEnvCommand, sparkClassPath, javaCommand, ytsaurusJavaOptions)
  }

  private val expectedExecutorCommand = "./setup-spyt-env.sh --some-key some-value && " +
    "'/usr/bin/java' '-cp' './*:/usr/lib/spyt/conf/:/usr/lib/spyt/jars/*:/usr/lib/spark/jars/*' '-Xmx1024m' " +
    "'-Dspark.driver.port=12345'   " +
    "org.apache.spark.executor.YTsaurusCoarseGrainedExecutorBackend " +
    """--driver-url 'spark://CoarseGrainedScheduler@some-host:12345' --executor-id "$YT_TASK_JOB_INDEX" """ +
    """--cores '1' --app-id 'appId' --hostname "$HOSTNAME""""

  test("Generate application files for python spark-submit in cluster mode") {
    val conf = new SparkConf()
    conf.set(SUBMIT_PYTHON_FILES, Seq("yt:/path/to/my/super/lib.zip"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.py")

    val result = YTsaurusOperationManager.applicationFiles(conf, identity)

    result should contain theSameElementsAs
      Seq(ApplicationFile("//path/to/my/super/app.py"), ApplicationFile("//path/to/my/super/lib.zip"))
  }

  test("Generate application files for python spark-submit in client mode") {
    val conf = new SparkConf()
    conf.set(FILES, Seq("yt:/path/to/my/super/lib.zip"))
    conf.set(SUBMIT_PYTHON_FILES, Seq("/tmp/spark-164a106b-cc57-4bb6-b30f-e67b7bbb8d8a/lib.zip"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.py")

    val result = YTsaurusOperationManager.applicationFiles(conf, identity)

    result should contain theSameElementsAs
      Seq(ApplicationFile("//path/to/my/super/app.py"), ApplicationFile("//path/to/my/super/lib.zip"))
  }

  test("Generate application files for java spark-submit") {
    val conf = new SparkConf()
    conf.set(JARS, Seq("yt:/path/to/my/super/lib.jar", "yt:///path/to/my/super/app.jar"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.jar")

    val result = YTsaurusOperationManager.applicationFiles(conf, identity)

    result should contain theSameElementsAs
      Seq(ApplicationFile("//path/to/my/super/lib.jar"), ApplicationFile("//path/to/my/super/app.jar"))
  }

  test("Generate application files for archives") {
    val conf = new SparkConf()
    conf.set(ARCHIVES, Seq("yt:/path/lib.tar.gz#unpacked", "yt:///path/lib2.zip"))
    conf.set(SUBMIT_PYTHON_FILES, Seq("yt:/path/to/lib.py#dep.py"))
    conf.set(SPARK_PRIMARY_RESOURCE, SparkLauncher.NO_RESOURCE)

    val result = YTsaurusOperationManager.applicationFiles(conf, identity)

    result should contain theSameElementsAs
      Seq(ApplicationFile("//path/lib.tar.gz", Some("unpacked"), isArchive = true),
        ApplicationFile("//path/lib2.zip", isArchive = true), ApplicationFile("//path/to/lib.py", Some("dep.py")))
  }

  test("Generate application files for spark-shell") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")

    val result = YTsaurusOperationManager.applicationFiles(conf, identity)

    result shouldBe empty
  }

  test("Generate application files when using local files that must be uploaded") {
    val conf = new SparkConf()
    conf.set(SUBMIT_PYTHON_FILES, Seq("dep.py"))
    conf.set(SPARK_PRIMARY_RESOURCE, "my-job.py")
    val uploader = (path: String) => s"uploaded-$path"

    val result = YTsaurusOperationManager.applicationFiles(conf, uploader)

    result should contain theSameElementsAs Seq(
      ApplicationFile("uploaded-dep.py", Some("dep.py")), ApplicationFile("uploaded-my-job.py", Some("my-job.py"))
    )
  }

  test("Test layer_paths override") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")
    conf.set(YTSAURUS_PORTO_LAYER_PATHS, "//path/to/layers/1,//path/to/layers/2")

    val result = YTsaurusOperationManager.getLayerPaths(conf, YTree.mapBuilder().buildMap(), "spark.tgz")
      .asList().asScala.map(x => x.stringValue())

    result should contain theSameElementsAs Seq("//path/to/layers/1", "//path/to/layers/2")
  }

  test("Test layer_paths override + extra layers") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")
    val releaseConfig: YTreeMapNode = YTree.mapBuilder()
      .key("layer_paths").beginList().value("//path/to/default_layers/1").value("//path/to/default_layers/2").endList()
      .buildMap()
    conf.set(YTSAURUS_PORTO_LAYER_PATHS, "//path/to/layers/1,//path/to/layers/2")
    conf.set(YTSAURUS_EXTRA_PORTO_LAYER_PATHS, "//path/to/extra_layers/3,//path/to/extra_layers/4")

    val result = YTsaurusOperationManager.getLayerPaths(conf, releaseConfig, "spark.tgz").asList().asScala.map(x => x.stringValue())

    result should contain theSameElementsAs Seq(
      "//path/to/extra_layers/3",
      "//path/to/extra_layers/4",
      "//path/to/layers/1",
      "//path/to/layers/2",
    )
  }

  test("Test layer_paths only by default & extra layers") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")
    conf.set(YTSAURUS_EXTRA_PORTO_LAYER_PATHS, "//path/to/extra_layers/3,//path/to/extra_layers/4")
    val releaseConfig: YTreeMapNode = YTree.mapBuilder()
      .key("layer_paths").beginList().value("//path/to/default_layers/1").value("//path/to/default_layers/2").endList()
      .buildMap()

    val result = YTsaurusOperationManager.getLayerPaths(conf, releaseConfig, "spark.tgz").asList().asScala.map(x => x.stringValue())

    result should contain theSameElementsAs Seq(
      "//path/to/extra_layers/3",
      "//path/to/extra_layers/4",
      "//path/to/default_layers/1",
      "//path/to/default_layers/2",
    )
  }

  test("Test layer_paths and file_paths for enables squashfs") {
    val conf = new SparkConf()
    conf.set(YTSAURUS_SQUASHFS_ENABLED, true)

    val releaseConfig: YTreeMapNode = YTree.mapBuilder()
      .key("spark_yt_base_path").value("//base/path")
      .key("layer_paths").beginList().value("//path/to/default_layers/1").value("//path/to/default_layers/2").endList()
      .key("squashfs_layer_paths").beginList().value("//path/to/squashfs_layers/1").value("//path/to/squashfs_layers/2").endList()
      .buildMap()

    val result = YTsaurusOperationManager.getLayerPaths(conf, releaseConfig, "//path/to/spark.squashfs").asList().asScala.map(x => x.stringValue())

    result should contain theSameElementsAs Seq(
      "//base/path/spyt-package.squashfs",
      "//path/to/spark.squashfs",
      "//path/to/squashfs_layers/1",
      "//path/to/squashfs_layers/2",
    )
  }

  test("Generate empty annotations for driver ans executors") {
    val conf = confForAnnotationTests()
    val emptyStructure = YTree.mapBuilder().buildMap()
    SpecificationUtils.getAnnotationsAsYTreeMapNode(conf, DRIVER_TASK) shouldBe emptyStructure
    SpecificationUtils.getAnnotationsAsYTreeMapNode(conf, EXECUTOR_TASK) shouldBe emptyStructure
  }

  test("Merge 2 ways of passing annotations") {
    val conf = confForAnnotationTests()
      .set(SPYT_ANNOTATIONS + ".key1" + ".n1", "123")
      .set(SPYT_ANNOTATIONS + ".key1" + ".n2", "common_annotation_n2")
      .set(SPYT_DRIVER_ANNOTATIONS + ".key2", "driver_annotation,driver_annotation_2")
      .set(SPYT_EXECUTORS_ANNOTATIONS + ".key3", "true")
      .set(SPYT_EXECUTORS_ANNOTATIONS + ".key4", "executors_annotation_2")
      .set("spark.ytsaurus.driver.operation.parameters", "{smth={qwerty=123};annotations={key1={n3=456; n4=common_driver_annotation_n4};" +
        "key2=[driver_annotation_3; driver_annotation_4]; key5=value_5}}")
      .set("spark.ytsaurus.executor.operation.parameters", "{annotations={key3=%false;key10=%true}}")

    val driverAnnotationsYtree = SpecificationUtils.getAnnotationsAsYTreeMapNode(conf, DRIVER_TASK)
    driverAnnotationsYtree shouldBe YTree.mapBuilder()
      .key("key1").value(
        YTree.mapBuilder()
          .key("n1").value(123)
          .key("n2").value("common_annotation_n2")
          .key("n3").value(456)
          .key("n4").value("common_driver_annotation_n4")
          .buildMap()
      )
      .key("key2").value(
        YTree.listBuilder()
          .value(YTree.stringNode("driver_annotation"))
          .value(YTree.stringNode("driver_annotation_2"))
          .value(YTree.stringNode("driver_annotation_3"))
          .value(YTree.stringNode("driver_annotation_4"))
          .buildList()
      )
      .key("key5").value("value_5")
      .buildMap()

    val executorAnnotationsYtree = SpecificationUtils.getAnnotationsAsYTreeMapNode(conf, EXECUTOR_TASK)
    executorAnnotationsYtree shouldBe YTree.mapBuilder()
      .key("key1").value(
        YTree.mapBuilder()
          .key("n1").value(123)
          .key("n2").value("common_annotation_n2")
          .buildMap()
      )
      .key("key3").value(true)
      .key("key4").value("executors_annotation_2")
      .key("key10").value(true)
      .buildMap()
  }

  test("Generate executor task specification with additional parameters") {
    val conf = testSparkConf.clone()
    conf.set("spark.ytsaurus.executor.task.parameters", "{ disk_request={ disk_space=536870912000 } }")

    val opManager = createYTsaurusOperationManagerStub()
    val execOpParams = opManager.executorParams(conf, "appId", testResourceProfile, 5)
    val result = execOpParams.taskSpec.prepare(YTree.builder(), null, null).build().asMap()
    result.containsKey("disk_request") shouldBe true
    result.get("disk_request").asMap().get("disk_space").longValue() shouldBe 536870912000L
    result.get("command").stringValue() shouldEqual expectedExecutorCommand
  }

  test("Additional task parameters should not override base task parameters") {
    val conf = testSparkConf.clone()
    conf.set(
      "spark.ytsaurus.executor.task.parameters",
      """{ command="/some/malicious/command"; disk_request={ disk_space=536870912000 } }"""
    )

    val opManager = createYTsaurusOperationManagerStub()
    val execOpParams = opManager.executorParams(conf, "appId", testResourceProfile, 5)
    val result = execOpParams.taskSpec.prepare(YTree.builder(), null, null).build().asMap()
    result.containsKey("disk_request") shouldBe true
    result.get("disk_request").asMap().get("disk_space").longValue() shouldBe 536870912000L
    result.get("command").stringValue() shouldEqual expectedExecutorCommand
  }

  test("createSpec should work") {
    val conf = confForAnnotationTests()

    val opManager = createYTsaurusOperationManagerStub()
    val execOpParams = opManager.executorParams(conf, "appId", testResourceProfile, 5)
    opManager.createSpec(conf, "executor", execOpParams).getAdditionalSpecParameters.get("secure_vault").asMap() shouldBe Map(
      "YT_TOKEN" -> YTree.stringNode("testToken"),
      "YT_USER" -> YTree.stringNode("testUser"),
    ).asJava
  }

  test("It should be possible to set secure vault") {
    val conf = confForAnnotationTests()
      .set("spark.ytsaurus.executor.operation.parameters", "{secure_vault={docker_auth=docker_auth}}")

    val opManager = createYTsaurusOperationManagerStub()
    val execOpParams = opManager.executorParams(conf, "appId", testResourceProfile, 5)
    opManager.createSpec(conf, "executor", execOpParams).getAdditionalSpecParameters.get("secure_vault").asMap() shouldBe Map(
      "YT_TOKEN" -> YTree.stringNode("testToken"),
      "YT_USER" -> YTree.stringNode("testUser"),
      "docker_auth" -> YTree.stringNode("docker_auth"),
    ).asJava
  }

  test("It should be possible to set executor secure vault docker_auth") {
    val conf = confForAnnotationTests()
      .set("spark.ytsaurus.executor.task.parameters", """{secure_vault={docker_auth={username="user";password="pass"}}}""")

    val opManager = createYTsaurusOperationManagerStub()
    val execOpParams = opManager.driverParams(conf, ApplicationArguments.fromCommandLineArgs(Array("--main-class", "Main")))
    val command = execOpParams.taskSpec.prepare(YTree.builder(), null, null).build().asMap().get("command").stringValue()
    command should include("""'-Dspark.ytsaurus.executor.task.parameters={secure_vault={docker_auth={username="user";password="pass"}}}'""")
  }

  def confForAnnotationTests(): SparkConf = {
    new SparkConf()
      .set("spark.app.name", "test-app-name")
  }
}