package org.apache.spark.sql.yt

import org.apache.hadoop.fs.Path
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yt.ReadTransactionStrategy.ypathEnriched
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient}
import tech.ytsaurus.spyt.format.GlobalTransactionUtils
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.Utils.tryWithResources
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import scala.concurrent.duration.DurationInt

class ReadTransactionStrategy(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val executionIdString =
      sparkSession.sparkContext.getLocalProperty(org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      return plan
    }
    val executionId = executionIdString.toLong
    if (GlobalTransactionUtils.getGlobalTransactionId(sparkSession).isEmpty && plan.find {
      case DataSourceV2Relation(table: YtTable, _, _, _, _) => table.paths.exists(ypathEnriched(_).transaction.isEmpty)
      case _ => false
    }.isDefined) {
      val listener = new SparkListener() {
        {
          sparkSession.sparkContext.addSparkListener(this)
        }
        implicit val ytClient: CompoundClient = YtClientProvider.ytClient(
          tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration(sparkSession.sessionState.conf)
        )
        val transaction: ApiServiceTransaction = YtWrapper.createTransaction(None, 2.minutes)

        override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
          case sqlExecutionEnd: SparkListenerSQLExecutionEnd if sqlExecutionEnd.executionId == executionId =>
            tryWithResources(ytClient) { _ =>
              tryWithResources(transaction) { _ => sparkSession.sparkContext.removeSparkListener(this) }
            }
          case _ =>
        }
      }
      plan.transform { case relation@DataSourceV2Relation(table: YtTable, _, _, _, _) =>
        relation.copy(table = table.copy(paths = table.paths.map(path => {
          val yPathEnriched = ypathEnriched(path)
          if (yPathEnriched.transaction.isEmpty) {
            yPathEnriched.withTransaction(listener.transaction.getId.toString).lock()(listener.ytClient).toStringPath
          } else {
            path
          }
        }
        )))
      }
    } else {
      plan
    }
  }
}

object ReadTransactionStrategy {
  def ypathEnriched(path: String): YPathEnriched = YPathEnriched.fromPath(new Path(path))
}
