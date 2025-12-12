package tech.ytsaurus.spyt.test

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions

import scala.sys.process._
import scala.util.{Failure, Success, Try}

class ExecutorKillerSparkListener(victimProcess: ProcessHandle,
  stageCondition: SparkListenerStageSubmitted => Boolean,
  taskCondition: SparkListenerTaskStart => Boolean
) extends SparkListener {

  private var alreadyShot = false
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (stageCondition(stageSubmitted)) {
      killExecutor()
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    if (taskCondition(taskStart)) {
      killExecutor()
    }
  }

  private def killExecutor(): Unit = {
    if (!alreadyShot) {
      victimProcess.destroyForcibly()
      alreadyShot = true
    }
  }
}

object ExecutorKillerSparkListener extends Assertions {
  def scheduleExecutorKillByStage(spark: SparkSession)(stageCondition: SparkListenerStageSubmitted => Boolean): Unit = {
    scheduleExecutorKill(spark, stageCondition = stageCondition)
  }

  def scheduleExecutorKillByTask(spark: SparkSession)(taskCondition: SparkListenerTaskStart => Boolean): Unit = {
    scheduleExecutorKill(spark, taskCondition = taskCondition)
  }

  private def scheduleExecutorKill(
    spark: SparkSession,
    stageCondition: SparkListenerStageSubmitted => Boolean = _ => false,
    taskCondition: SparkListenerTaskStart => Boolean = _ => false): Unit = {
    // Since we're using here local-cluster Spark master hence Executors are child processes of the test process.
    // So here we are choosing one child executor process as a victim to kill it later.
    val executorProcessToKillOpt = ProcessHandle.current().descendants().filter { child =>
      Try(Seq("cat", s"/proc/${child.pid()}/cmdline").!!) match {
        case Success(procCmd) => procCmd.contains("CoarseGrainedExecutorBackend")
        case Failure(exception) =>
          println("Got an exception during getting child processes command")
          exception.printStackTrace(System.out)
          false
      }
    }.findAny()

    if (executorProcessToKillOpt.isEmpty) {
      fail("No executor child processes is found, there must be at least one")
    }
    val executorProcessToKill = executorProcessToKillOpt.get()
    val executorKillerListener = new ExecutorKillerSparkListener(executorProcessToKill, stageCondition, taskCondition)

    spark.sparkContext.addSparkListener(executorKillerListener)
  }
}
