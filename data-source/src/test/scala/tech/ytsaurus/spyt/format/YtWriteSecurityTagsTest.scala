package tech.ytsaurus.spyt.format

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration.InferredSecurityTags
import tech.ytsaurus.spyt.wrapper.config.OptionsConf
import tech.ytsaurus.ysontree.YTree

import scala.jdk.CollectionConverters._

class YtWriteSecurityTagsTest extends AnyFlatSpec with Matchers {

  it should "attach a typed list while preserving the other write path attributes" in {
    val path = YPath.simple("//tmp/output").append(true)
    val result = YtWriteSecurityTags.withTags(path, Map(InferredSecurityTags.name -> "[b;a;b]"))
    result.getAdditionalAttribute("security_tags").get() shouldBe YTree.node(Seq("a", "b").asJava)
    result.getAppend.get() shouldBe true
  }

  it should "distinguish an empty explicit override from missing tags" in {
    YtWriteSecurityTags.resolve(Map.empty[String, String]) shouldBe None
    val options = Map(InferredSecurityTags.name -> "[input]", "attr_security_tags" -> "[]")
    YtWriteSecurityTags.resolve(options) shouldBe Some(Nil)
  }

  Seq("[primary]" -> Seq("primary"), "[]" -> Seq.empty[String]).foreach { case (value, expected) =>
    it should s"prefer security_tags=$value over its alias and inferred tags" in {
      val options = Map(
        "security_tags" -> value,
        "attr_security_tags" -> "[alias]",
        InferredSecurityTags.name -> "[input]")
      val result = YtWriteSecurityTags.withTags(YPath.simple("//tmp/output"), options)
      result.getAdditionalAttribute("security_tags").get() shouldBe YTree.node(expected.asJava)
    }
  }

  Seq("plain-string", "[1]", "[\"\"]", "[\"" + "a" * 129 + "\"]", "[\"" + "é" * 65 + "\"]").foreach { value =>
    it should s"reject invalid security tags $value" in {
      an[IllegalArgumentException] should be thrownBy {
        YtWriteSecurityTags.resolve(Map("security_tags" -> value))
      }
    }
  }
}
