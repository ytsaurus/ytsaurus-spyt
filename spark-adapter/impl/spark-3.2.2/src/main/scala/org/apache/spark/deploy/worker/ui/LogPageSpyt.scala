package org.apache.spark.deploy.worker.ui

import org.apache.spark.ui.UIUtils
import tech.ytsaurus.spyt.patch.annotations.{Applicability, OriginClass, Subclass}

import javax.servlet.http.HttpServletRequest
import scala.xml.{Elem, Node}

/**
 * This is a fix for a bug in spark 3.2.2 which doesn't include utils.js file on logPage, but uses a getBaseURI
 * function from it. This bug still exists in versions 3.2.x, 3.3.x (except 3.3.4) and 3.4.x (was fixed since 3.4.2).
 */
@Subclass
@OriginClass("org.apache.spark.deploy.worker.ui.LogPage")
@Applicability(to = "3.4.1")
class LogPageSpyt(parent: WorkerWebUI) extends LogPage(parent) {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content = super.render(request)
    val utilsJs = <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script>
    val html = content.head
    val head = html.child.find {
      case e: Elem if e.label == "head" => true
      case _ => false
    }.get
    val body = html.child.find {
      case e: Elem if e.label == "body" => true
      case _ => false
    }.get
    <html><head>{head.child ++ utilsJs}</head>{body}</html>
  }
}
