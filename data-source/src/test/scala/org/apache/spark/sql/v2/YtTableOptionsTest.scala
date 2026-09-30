package org.apache.spark.sql.v2

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class YtTableOptionsTest extends AnyFlatSpec with Matchers {
  behavior of "YtTable.mergeScanOptions"

  it should "let scan options override table options with the same key in any case and keep the others" in {
    val table = Map("readparallelism" -> "1", "path" -> "/t", "arrow_enabled" -> "false")
    val scan = Map("readParallelism" -> "8", "path" -> "/t")
    val merged = YtTable.mergeScanOptions(
      new CaseInsensitiveStringMap(table.asJava),
      new CaseInsensitiveStringMap(scan.asJava))
    merged.asCaseSensitiveMap.asScala shouldEqual Map("readParallelism" -> "8", "path" -> "/t", "arrow_enabled" -> "false")
  }
}
