package tech.ytsaurus.spyt.fs.path

import org.apache.hadoop.fs.Path
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.GUID
import tech.ytsaurus.core.cypress.{RichYPath, YPath}
import tech.ytsaurus.spyt.fs.path.YPathEnriched._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree.YTreeBuilder

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable

case class YPathEnriched(path: Path, attributes: Map[String, String] = Map.empty) extends Serializable {
  implicit class RichPath(val path: Path) {
    def child(name: String): Path = {
      new Path(path.toString + Path.SEPARATOR + name)
    }
  }

  def toPath: Path = attributes.foldLeft(path) { case (p, (k, v)) => p.child(s"@${k}_$v") }

  def toStringPath: String = toPath.toString

  def toYPath: YPath = {
    import scala.collection.JavaConverters._

    val yPath = node.map(n => YPath.objectRoot(GUID.valueOf(n))).getOrElse(YPath.simple(YtWrapper.formatPath(path.toString)))
    val tsYPath = timestamp.filter(_ != -1).map(yPath.withTimestamp).getOrElse(yPath)
    val attrs = attributes.filterKeys(!RESERVED_ATTRIBUTES.contains(_)).mapValues(new YTreeBuilder().value(_).build())
    tsYPath.withAdditionalAttributes(attrs.asJava)
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

  def lock()(implicit yt: CompoundClient): YPathEnriched = transaction match {
    case Some(tId) if node.isEmpty => withAttr(NODE_KEY, YtWrapper.lockNode(toYPath, tId))
    case _ => this
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
      fromPath(path.getParent, attrs :+ (attr.head -> attr.last))
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
    val attrs = mutable.ListMap[String, String]()
    val cluster = richP.getAdditionalAttribute("cluster")
    if (cluster.isPresent) {
      attrs("cluster") = cluster.get().stringValue()
    }
    YPathEnriched(new Path(scheme, null, strP), attrs.toMap)
  }
}

