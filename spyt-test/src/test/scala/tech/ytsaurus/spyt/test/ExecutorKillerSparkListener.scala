package tech.ytsaurus.spyt.test

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions

import scala.sys.process._
import scala.util.{Failure, Success, Try}

class ExecutorKillerSparkListener(victimProcess: ProcessHandle,
  condition: SparkListenerStageSubmitted => Boolean) extends SparkListener {

  private var alreadyShot = false
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val isLastStage = condition(stageSubmitted)

    if (isLastStage && !alreadyShot) {
      victimProcess.destroyForcibly()
      alreadyShot = true
    }
  }
}

object ExecutorKillerSparkListener extends Assertions {
  def scheduleExecutorKill(spark: SparkSession)(condition: SparkListenerStageSubmitted => Boolean): Unit = {
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
    val executorKillerListener = new ExecutorKillerSparkListener(executorProcessToKill, condition)

    spark.sparkContext.addSparkListener(executorKillerListener)
  }
}
