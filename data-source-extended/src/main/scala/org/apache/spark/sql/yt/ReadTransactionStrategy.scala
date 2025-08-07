package org.apache.spark.sql.yt

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yt.ReadTransactionStrategy._
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient}
import tech.ytsaurus.spyt.format.GlobalTransactionUtils
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.Utils.tryWithResources
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientConfigurationConverter, YtClientProvider}
import tech.ytsaurus.spyt.wrapper.config._

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class ReadTransactionStrategy(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val disabled = !sparkSession.sparkContext.conf.ytConf(SparkYtConfiguration.Read.Transactional)
    val executionIdString = sparkSession.sparkContext.getLocalProperty(EXECUTION_ID_KEY)
    val underGlobalTransaction = GlobalTransactionUtils.getGlobalTransactionId(sparkSession).isDefined

    if (disabled || executionIdString == null || underGlobalTransaction || allPathsWithTransaction(plan)) {
      return plan
    }
    val executionId = executionIdString.toLong
    val configuration = YtClientConfigurationConverter.ytClientConfiguration(sparkSession.sessionState.conf)

    val listeners = mutable.HashMap[String, TransactionListener]()
    plan.transform {
      case relation: DataSourceV2Relation if relation.table.isInstanceOf[YtTable] =>
        val table = relation.table.asInstanceOf[YtTable]
        relation.copy(table = table.copy(paths = table.paths.map(path => {
          val yPathEnriched = ypathEnriched(path)
          val proxy = yPathEnriched.cluster.getOrElse(configuration.proxy)
          val transactionListener = listeners.getOrElseUpdate(proxy,
            new TransactionListener(proxy, sparkSession.sparkContext, configuration, executionId)
          )
          transactionListener.lockPath(yPathEnriched).toStringPath
        })))
    }
  }
}

object ReadTransactionStrategy {
  private def ypathEnriched(path: String): YPathEnriched = YPathEnriched.fromPath(new Path(path))

  private def allPathsWithTransaction(plan: LogicalPlan) = plan.find {
    case relation: DataSourceV2Relation if relation.table.isInstanceOf[YtTable] =>
      relation.table.asInstanceOf[YtTable].paths.exists(path => ypathEnriched(path).transaction.isEmpty)
    case _ => false
  }.isEmpty

  private class TransactionListener(
    proxy: String,
    sc: SparkContext,
    configuration: YtClientConfiguration,
    executionId: Long) extends SparkListener {

    private val ytRpcClient = YtClientProvider.ytRpcClient(configuration.replaceProxy(Some(proxy)))
    private implicit val ytClient: CompoundClient = ytRpcClient.yt
    private val transaction: ApiServiceTransaction = YtWrapper.createTransaction(None, 2.minutes)

    sc.addSparkListener(this)

    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case sqlExecutionEnd: SparkListenerSQLExecutionEnd if sqlExecutionEnd.executionId == executionId =>
        tryWithResources(transaction) { _ => sc.removeSparkListener(this) }
      case _ =>
    }

    def lockPath(yPathEnriched: YPathEnriched): YPathEnriched =
      if (yPathEnriched.transaction.isEmpty) {
        yPathEnriched.withTransaction(transaction.getId.toString)
      } else {
        yPathEnriched
      }
  }
}
