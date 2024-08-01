
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.deploy.ytsaurus.Config.{SPARK_PRIMARY_RESOURCE, YTSAURUS_EXTRA_PORTO_LAYER_PATHS, YTSAURUS_PORTO_LAYER_PATHS}
import org.apache.spark.internal.config.{FILES, JARS, SUBMIT_PYTHON_FILES}
import org.apache.spark.deploy.ytsaurus.Config.SPARK_PRIMARY_RESOURCE
import org.apache.spark.internal.config.{ARCHIVES, FILES, JARS, SUBMIT_PYTHON_FILES}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.ysontree.YTree

import scala.collection.JavaConverters._

class YTsaurusOperationManagerSuite extends SparkFunSuite with BeforeAndAfter with Matchers {

  test("Generate application files for python spark-submit in cluster mode") {
    val conf = new SparkConf()
    conf.set(SUBMIT_PYTHON_FILES, Seq("yt:/path/to/my/super/lib.zip"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.py")
    conf.set(ARCHIVES, Seq(
      "yt:///path/to/my/super/env.tar",
      "yt:///path/to/my/super/env.tar#env-alias",
      "yt:///path/to/my/super/env.tar.gz#tar-gz-env",
      "yt:///path/to/my/super/env.tgz#tgz-env",
      "yt:///path/to/my/super/env.tar.bz2#tar-bz2-env",
      "yt:///path/to/my/super/env.zip#zip-env",
    ))

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result should contain theSameElementsAs Seq(
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/app.py", None, None),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/lib.zip", None, None),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/env.tar", Some("__dep-0-env.tar"), Some("mkdir env.tar && tar -xvf __dep-0-env.tar -C env.tar")),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/env.tar", Some("__dep-1-env.tar"), Some("mkdir env-alias && tar -xvf __dep-1-env.tar -C env-alias")),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/env.tar.gz", Some("__dep-2-env.tar.gz"), Some("mkdir tar-gz-env && tar -xzvf __dep-2-env.tar.gz -C tar-gz-env")),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/env.tgz", Some("__dep-3-env.tgz"), Some("mkdir tgz-env && tar -xzvf __dep-3-env.tgz -C tgz-env")),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/env.tar.bz2", Some("__dep-4-env.tar.bz2"), Some("mkdir tar-bz2-env && tar -xjvf __dep-4-env.tar.bz2 -C tar-bz2-env")),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/env.zip", Some("__dep-5-env.zip"), Some("unzip __dep-5-env.zip -d zip-env")),
    )
  }

  test("Generate application files for python spark-submit in client mode") {
    val conf = new SparkConf()
    conf.set(FILES, Seq("yt:/path/to/my/super/lib.zip"))
    conf.set(SUBMIT_PYTHON_FILES, Seq("/tmp/spark-164a106b-cc57-4bb6-b30f-e67b7bbb8d8a/lib.zip"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.py")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result should contain theSameElementsAs Seq(
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/app.py", None, None),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/lib.zip", None, None)
    )
  }

  test("Generate application files for java spark-submit") {
    val conf = new SparkConf()
    conf.set(JARS, Seq("yt:/path/to/my/super/lib.jar", "yt:///path/to/my/super/app.jar"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.jar")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result should contain theSameElementsAs Seq(
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/lib.jar", None, None),
      new YTsaurusOperationManager.ApplicationFile("//path/to/my/super/app.jar", None, None)
    )
  }

  test("Generate application files for spark-shell") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result shouldBe empty
  }

  test("Test layer_paths override") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")
    conf.set(YTSAURUS_PORTO_LAYER_PATHS, "//path/to/layers/1,//path/to/layers/2")

    val result = YTsaurusOperationManager.getPortoLayers(conf, YTree.listBuilder().buildList()).asList().asScala.map(x => x.stringValue())

    result should contain theSameElementsAs Seq("//path/to/layers/1", "//path/to/layers/2")
  }

  test("Test layer_paths override + extra layers") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")
    conf.set(YTSAURUS_PORTO_LAYER_PATHS, "//path/to/layers/1,//path/to/layers/2")
    conf.set(YTSAURUS_EXTRA_PORTO_LAYER_PATHS, "//path/to/layers/3,//path/to/layers/4")

    val result = YTsaurusOperationManager.getPortoLayers(conf, YTree.listBuilder().buildList()).asList().asScala.map(x => x.stringValue())

    result should contain theSameElementsAs Seq(
      "//path/to/layers/1",
      "//path/to/layers/2",
      "//path/to/layers/3",
      "//path/to/layers/4",
    )
  }

}
