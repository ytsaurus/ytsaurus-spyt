package tech.ytsaurus.spyt.fs.path

import org.apache.hadoop.fs.Path
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.GUID
import tech.ytsaurus.core.cypress.{RichYPath, YPath}
import tech.ytsaurus.spyt.fs.path.YPathEnriched._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode}

import java.util.concurrent.CompletableFuture
import java.util.function.{Function => JFunction}
import java.util.stream.Collectors
import java.util.{Map => JMap}
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

case class YPathEnriched(path: Path, attributes: Map[String, String] = Map.empty) extends Serializable {
  implicit class RichPath(val path: Path) {
    def child(name: String): Path = {
      new Path(path.toString + Path.SEPARATOR + name)
    }
  }

  def toPath: Path = {
    var pathWithAttributes = path
    attributes.foreach { case (k, v) => pathWithAttributes = pathWithAttributes.child(s"@${k}_$v") }
    pathWithAttributes
  }

  def toStringPath: String = toPath.toString

  def toYPath: YPath = {
    val yPath = node.map(n => YPath.objectRoot(GUID.valueOf(n))).getOrElse(YPath.simple(YtWrapper.formatPath(path.toString)))
    val tsYPath = timestamp.filter(_ != -1).map(yPath.withTimestamp).getOrElse(yPath)
    val keyMapper: JFunction[JMap.Entry[String, String], String] = (e: JMap.Entry[String, String]) => e.getKey
    val valueMapper: JFunction[JMap.Entry[String, String], YTreeNode] =
      (e: JMap.Entry[String, String]) => new YTreeBuilder().value(e.getValue).build()
    val ypathAttributes: JMap[String, YTreeNode] = attributes.asJava
      .entrySet()
      .stream()
      .filter(e => !RESERVED_ATTRIBUTES.contains(e.getKey))
      .collect(Collectors.toMap[JMap.Entry[String, String], String, YTreeNode](keyMapper, valueMapper))
    tsYPath.withAdditionalAttributes(ypathAttributes)
  }

  def toStringYPath: String = toYPath.toStableString

  def cluster: Option[String] = attributes.get(CLUSTER_KEY)

  def transaction: Option[String] = attributes.get(TRANSACTION_KEY)

  def timestamp: Option[Long] = attributes.get(TIMESTAMP_KEY).map(_.toLong)

  def node: Option[String] = attributes.get(NODE_KEY)

  def parent: YPathEnriched = YPathEnriched(path.getParent, attributes)

  def name: String = path.getName

  def withName(newName: String): YPathEnriched = parent.child(newName)

  def child(name: String): YPathEnriched = YPathEnriched(path.child(name), attributes)

  def withAttr(key: String, value: String): YPathEnriched = YPathEnriched(path, attributes + (key -> value))

  def withTransaction(transactionId: String): YPathEnriched = withAttr(TRANSACTION_KEY, transactionId)

  def withTransaction(transactionId: Option[String]): YPathEnriched = transactionId.map(withTransaction).getOrElse(this)

  def withTimestamp(timestamp: Long): YPathEnriched = withAttr(TIMESTAMP_KEY, timestamp.toString)

  def withLatestVersion: YPathEnriched = withTimestamp(-1)

  def dropTimestamp(): YPathEnriched = YPathEnriched(path, attributes - TIMESTAMP_KEY)

  def lockAsync()(implicit yt: CompoundClient): CompletableFuture[YPathEnriched] = transaction match {
    case Some(tId) if node.isEmpty =>
      YtWrapper.lockNodeAsync(toYPath, tId).thenApply(nodeId => withAttr(NODE_KEY, nodeId))
    case _ =>
      CompletableFuture.completedFuture(this)
  }

  override def equals(obj: Any): Boolean = obj match {
    case value: YPathEnriched => path.equals(value.path) && attributes.equals(value.attributes)
    case _ => false
  }

  override def toString: String = toStringPath
}

object YPathEnriched {
  private val NODE_KEY = "node"
  private val TIMESTAMP_KEY = "timestamp"
  private val TRANSACTION_KEY = "transaction"
  private val CLUSTER_KEY = "cluster"
  private val RESERVED_ATTRIBUTES: Set[String] = Set(NODE_KEY, TIMESTAMP_KEY, TRANSACTION_KEY, CLUSTER_KEY)

  @tailrec
  def fromPath(path: Path, attrs: Seq[(String, String)] = Seq.empty): YPathEnriched = {
    if (path.getName.startsWith("@")) {
      val attr = path.getName.drop(1).split("_", 2)
      fromPath(path.getParent, attrs :+ (attr.head, attr.last))
    } else {
      // NB: It's important to save order of attributes
      YPathEnriched(path, ListMap(attrs.reverse: _*))
    }
  }

  def fromString(p: String): YPathEnriched = {
    // It can be taken from path, but now ytTable is the only right schema
    val scheme = "ytTable"
    val path = if (p.contains(":/")) p.split(":/", 2).last else p
    val richP = RichYPath.fromString(path)
    val strP = YtWrapper.correctSlashes(richP.justPath().toStableString, 1)
    val cluster = richP.getAdditionalAttribute("cluster")
    val attrs: Map[String, String] = if (cluster.isPresent) {
      Map("cluster" -> cluster.get().stringValue())
    } else {
      Map.empty
    }
    YPathEnriched(new Path(scheme, null, strP), attrs)
  }
}

