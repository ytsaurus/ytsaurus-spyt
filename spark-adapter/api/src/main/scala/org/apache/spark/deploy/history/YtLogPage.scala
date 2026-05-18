package org.apache.spark.deploy.history

import org.apache.spark.ui.WebUIPage

import scala.xml.Node

abstract class YtLogPage(delegate: YtLogPageDelegate) extends WebUIPage("workerLogPage")

trait YtLogPageDelegate {
  def render(getParameter: String => String): (Seq[Node], String)
  def renderLog(getParameter: String => String): String
}
