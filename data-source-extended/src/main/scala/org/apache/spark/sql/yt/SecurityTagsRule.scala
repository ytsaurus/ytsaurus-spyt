package org.apache.spark.sql.yt

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.vectorized.YtFileFormat

import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration.InferredSecurityTags
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.wrapper.table.YtSecurityTags
import tech.ytsaurus.ysontree.YTreeTextSerializer

class SecurityTagsRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  import SecurityTagsRule._

  override def apply(plan: LogicalPlan): LogicalPlan = AnalysisHelper.allowInvokingTransformsInAnalyzer {
    // Provenance must include already analyzed subtrees, which resolveOperators skips.
    plan.transformUpWithSubqueries {
      case node if node.resolved =>
        // Capture provenance before cache substitution and optimizations can remove input relations.
        val tags = collectSecurityTags(node)
        node.setTagValue(securityTags, tags)
        withInferredWriteTags(node, tags)
    }
  }

  private def collectSecurityTags(node: LogicalPlan): Seq[String] = {
    val cachedTags = SparkAdapter.instance.lookupCachedPlan(sparkSession, node)
      .flatMap(_.getTagValue(securityTags))
    val sourceTags = node.getTagValue(securityTags).getOrElse {
      val inputTags = readTableTags(node)
      val childTags = (node.children ++ node.subqueries).flatMap(_.getTagValue(securityTags).toSeq.flatten)
      (inputTags ++ childTags).distinct.sorted
    }
    // Keep both versions: the cache may be evicted between analysis and execution.
    (sourceTags ++ cachedTags.getOrElse(Nil)).distinct.sorted
  }

  private def readTableTags(node: LogicalPlan): Seq[String] = node match {
    case relation: DataSourceV2Relation if relation.table.isInstanceOf[YtTable] =>
      relation.table.asInstanceOf[YtTable].fileIndex.allFiles().flatMap(file => tagsFromPath(file.getPath))
    case relation: LogicalRelation => relation.relation match {
      case table: HadoopFsRelation if table.fileFormat.isInstanceOf[YtFileFormat] =>
        table.location.inputFiles.toSeq.flatMap(path => tagsFromPath(new Path(path)))
      case _ => Nil
    }
    case _ => Nil
  }

  private def withInferredWriteTags(node: LogicalPlan, tags: Seq[String]): LogicalPlan = node match {
    case write: InsertIntoHadoopFsRelationCommand if write.fileFormat.isInstanceOf[YtFileFormat] =>
      val inferredTags = InferredSecurityTags.name -> YTreeTextSerializer.serialize(YtSecurityTags.toNode(tags))
      write.copy(options = write.options + inferredTags)
    case _ => node
  }
}

object SecurityTagsRule {

  private val securityTags = TreeNodeTag[Seq[String]]("yt.security_tags")

  private def tagsFromPath(path: Path): Seq[String] = YtHadoopPath.fromPath(path) match {
    case table: YtHadoopPath => table.meta.securityTags
    case _ => Nil
  }
}
