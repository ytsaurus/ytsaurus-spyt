package tech.ytsaurus.spyt.wrapper.cypress

import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.test.{LocalYtClient, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.test.LocalYtClient
import tech.ytsaurus.ysontree.YTreeBuilder

import java.util.UUID
import scala.collection.JavaConverters._

class YtCypressUtilsTest extends FlatSpec with Matchers with LocalYtClient with TmpDir {
  behavior of "YtCypressUtils"

  import tech.ytsaurus.spyt.wrapper.YtJavaConverters._

  it should "createDocument" in {
    implicit val ysonWriter: YsonWriter[TestDoc] = (t: TestDoc) => {
      new YTreeBuilder().beginMap().key("a").value(t.a).key("b").value(t.b).endMap().build()
    }

    YtWrapper.createDocumentFromProduct(tmpPath, new TestDoc("A", 1))

    val res = YtWrapper.readDocument(tmpPath).asMap()
    res.keySet().asScala should contain theSameElementsAs Seq("a", "b")
    res.getOrThrow("a").stringValue() shouldEqual "A"
    res.getOrThrow("b").intValue() shouldEqual 1
  }

  it should "create document from case class" in {
    YtWrapper.createDocumentFromProduct(tmpPath, TestDocProduct("A", 1))

    val res = YtWrapper.readDocument(tmpPath).asMap()
    res.keySet().asScala should contain theSameElementsAs Seq("a", "b")
    res.getOrThrow("a").stringValue() shouldEqual "A"
    res.getOrThrow("b").intValue() shouldEqual 1
  }

  it should "format path" in {
    YtWrapper.formatPath("ytEventLog:///home/path") shouldEqual "//home/path"
    YtWrapper.formatPath("//home/path") shouldEqual "//home/path"
    YtWrapper.formatPath("/home/path") shouldEqual "//home/path"
    YtWrapper.formatPath("ytEventLog:/home/path") shouldEqual "//home/path"
    YtWrapper.formatPath(
      YtWrapper.formatPath("ytEventLog:///home/dev/alex-shishkin/spark-test/logs/event_log_table")
    ) shouldEqual "//home/dev/alex-shishkin/spark-test/logs/event_log_table"
    an[IllegalArgumentException] should be thrownBy {
      YtWrapper.formatPath("home/path")
    }
  }

  it should "escape path" in {
    // https://yt.yandex-team.ru/docs/description/common/ypath#simple_ypath_lexis
    val unescaped = "\\a/b@c&d*e[f{g"
    val escaped = YtWrapper.escape(unescaped)
    escaped shouldEqual "\\\\a\\/b\\@c\\&d\\*e\\[f\\{g"
  }

  it should "isDir should check if path is not directory" in {
    implicit val ysonWriter: YsonWriter[TestDoc] = (t: TestDoc) => {
      new YTreeBuilder().beginMap().key("a").value(t.a).key("b").value(t.b).endMap().build()
    }
    YtWrapper.createDocumentFromProduct(tmpPath, new TestDoc("A", 1))
    YtWrapper.isDir(tmpPath) shouldBe false
  }

  it should "isDir should check if path is directory" in {
    YtWrapper.createDir(tmpPath)
    YtWrapper.isDir(tmpPath) shouldBe true
  }

  it should "isDir should follow the links" in {
    YtWrapper.createDir(tmpPath)
    val tmpPath2 = s"$testDir/test-${UUID.randomUUID()}"
    try {
      YtWrapper.createLink(tmpPath, tmpPath2)
      YtWrapper.isDir(tmpPath2) shouldBe true
    } finally {
      YtWrapper.removeIfExists(tmpPath2)
    }
  }

  it should "createLink should create links" in {
    implicit val ysonWriter: YsonWriter[TestDoc] = (t: TestDoc) => {
      new YTreeBuilder().beginMap().key("a").value(t.a).key("b").value(t.b).endMap().build()
    }
    val tmpPath2 = s"$testDir/test-${UUID.randomUUID()}"
    try {
      YtWrapper.createDocumentFromProduct(tmpPath, new TestDoc("A", 1))
      YtWrapper.createLink(tmpPath, tmpPath2)
      YtWrapper.readDocument(tmpPath2).asMap().getOrThrow("a").stringValue() shouldEqual "A"
    }
    finally {
      YtWrapper.removeIfExists(tmpPath2)
    }
  }

  it should "createDir shouldn't fail if path exists and it is a directory link" in {
    YtWrapper.createDir(tmpPath)
    val tmpPath2 = s"$testDir/test-${UUID.randomUUID()}"
    try {
      YtWrapper.createLink(tmpPath, tmpPath2)
      YtWrapper.isDir(tmpPath2) shouldBe true
      YtWrapper.createDir(tmpPath2, ignoreExisting = true)
    } finally {
      YtWrapper.removeIfExists(tmpPath2)
    }
  }
}

case class TestDoc(a: String, b: Int)

case class TestDocProduct(a: String, b: Int)
