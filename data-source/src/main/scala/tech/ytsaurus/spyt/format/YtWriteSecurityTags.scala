package tech.ytsaurus.spyt.format

import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration.InferredSecurityTags
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.SecurityTags
import tech.ytsaurus.spyt.wrapper.config.ConfProvider
import tech.ytsaurus.spyt.wrapper.table.YtSecurityTags
import tech.ytsaurus.ysontree.YTree

import scala.jdk.CollectionConverters._

object YtWriteSecurityTags {

  def resolve(options: ConfProvider): Option[Seq[String]] = {
    options.getYtConf(SecurityTags).orElse(options.getYtConf(InferredSecurityTags))
      .map(YtSecurityTags.fromNode)
  }

  def withTags(path: YPath, options: ConfProvider): YPath = {
    resolve(options).fold(path) { tags =>
      path.plusAdditionalAttribute(YtSecurityTags.attribute, YTree.node(tags.asJava))
    }
  }
}
