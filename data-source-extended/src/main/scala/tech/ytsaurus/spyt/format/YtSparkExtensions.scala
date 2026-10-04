package tech.ytsaurus.spyt.format

import org.apache.spark.sql.yt.{ReadTransactionStrategy, SecurityTagsRule, WriteTransactionRule}
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.slf4j.LoggerFactory

import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.format.columnar.{RegisterColumnarFunction, YtColumnarUdfRule}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.ColumnarUdfEnabled
import tech.ytsaurus.spyt.format.optimizer.{YtSortedTableStrategy, YtSourceStrategy}

class YtSparkExtensions extends SparkSessionExtensionsProvider {

  private val log = LoggerFactory.getLogger(getClass)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    log.info("Apply YtSparkExtensions")
    extensions.injectPlannerStrategy(YtSortedTableStrategy(_))
    extensions.injectPreCBORule(new ReadTransactionStrategy(_))
    extensions.injectPostHocResolutionRule(new SecurityTagsRule(_))
    extensions.injectPostHocResolutionRule(new WriteTransactionRule(_))
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
    extensions.injectColumnar(session => new YtColumnarUdfRule(session))
    extensions.injectParser { (session, parser) =>
      if (ColumnarUdfEnabled.get(session.sparkContext.getConf.getOption(ColumnarUdfEnabled.name)).get) {
        SparkAdapter.instance.createYtColumnarFunctionParser(parser, RegisterColumnarFunction.apply)
      } else {
        parser
      }
    }
  }
}
