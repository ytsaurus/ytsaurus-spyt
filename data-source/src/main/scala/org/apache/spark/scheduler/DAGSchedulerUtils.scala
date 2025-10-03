package org.apache.spark.scheduler

import org.apache.spark.SparkContext

object DAGSchedulerUtils {
  def getNumOutputTasks(sc: SparkContext, stageIds: Seq[Int]): Option[Int] = {
    stageIds.map(sc.dagScheduler.stageIdToStage).collectFirst {
      case resultStage: ResultStage => resultStage.numTasks
    }
  }
}
