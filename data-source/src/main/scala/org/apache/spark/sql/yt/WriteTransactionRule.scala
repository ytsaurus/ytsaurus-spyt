package org.apache.spark.sql.yt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.v2.YtDataSourceV2
import org.apache.spark.sql.vectorized.YtFileFormat

import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.WriteTransaction
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.config._

class WriteTransactionRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case command: InsertIntoHadoopFsRelationCommand if command.fileFormat.isInstanceOf[YtFileFormat] =>
      val options = YtDataSourceV2.withWriteTransactionDefault(spark.sessionState.conf, command.options)
      val transaction = options.getYtConf(WriteTransaction).filter(_.nonEmpty)
      // Spark checks output existence before prepareWrite, including for overwrite and ignore modes.
      val path = YPathEnriched.fromPath(command.outputPath).withTransaction(transaction)
      command.copy(outputPath = path.toPath, options = options)
  }
}
