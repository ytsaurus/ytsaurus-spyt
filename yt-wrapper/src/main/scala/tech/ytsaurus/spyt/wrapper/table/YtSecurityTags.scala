package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._

object YtSecurityTags {

  val attribute = "security_tags"

  def fromNode(node: YTreeNode): Seq[String] = {
    require(node.isListNode, s"Security tags must be a YSON list of strings, got $node.")
    val tags = node.asList().asScala.map { tag =>
      require(tag.isStringNode, s"Each security tag must be a string, got $tag.")
      val value = tag.stringValue()
      val length = value.getBytes(UTF_8).length
      require(
        length > 0 && length <= 128,
        s"Security tags must contain between 1 and 128 UTF-8 bytes, got a tag of length $length.")
      value
    }
    tags.toSeq.distinct.sorted
  }

  def toNode(tags: Seq[String]): YTreeNode = YTree.node(tags.distinct.sorted.asJava)

  def fromAttributes(attributes: Map[String, YTreeNode]): Seq[String] = {
    attributes.get(attribute).map(fromNode).getOrElse(Nil)
  }
}
