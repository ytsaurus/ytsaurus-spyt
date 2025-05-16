package tech.ytsaurus.spyt.submit

import org.apache.spark.deploy.ytsaurus.Config.{SUBMISSION_ID, YTSAURUS_DRIVER_WATCH}
import org.apache.spark.deploy.ytsaurus.YTsaurusClusterApplication
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{InProcessLauncher, SparkAppHandle}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Try

class DirectSubmitter extends Logging  {

  private class SubmissionSparkListener(val submissionId: String) extends SparkAppHandle.Listener{
    override def stateChanged(handle: SparkAppHandle): Unit = {
      if(handle.getState == SparkAppHandle.State.FAILED){
        val error = handle.getError.orElse(new Exception("Unknown error in SparkAppHandle"))
        YTsaurusClusterApplication.failOperationId(submissionId, error)
      }
    }

    override def infoChanged(handle: SparkAppHandle): Unit = ()
  }

  def submit(launcher: InProcessLauncher, timeoutSec: Int = 30): Try[String] = {
    val submissionId = UUID.randomUUID().toString
    val operationIdPromise = YTsaurusClusterApplication.getOrCreatePromise(submissionId)
    launcher.setConf(SUBMISSION_ID, submissionId)
    launcher.setConf(YTSAURUS_DRIVER_WATCH.key, "false")
    launcher.startApplication(new SubmissionSparkListener(submissionId))
    Try {
      Await.result(operationIdPromise.future, timeoutSec.seconds)
    }
  }
}
