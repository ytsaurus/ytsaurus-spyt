package org.apache.spark.api.python.spyt.patch

import java.util.{List => JList, Map => JMap}
import org.apache.spark.api.python.{PythonAccumulatorV2, PythonBroadcast, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import tech.ytsaurus.spyt.patch.annotations.{Applicability, OriginClass}

@OriginClass("org.apache.spark.api.python.SimplePythonFunction")
@Applicability(from = "3.4.0")
private[spark] case class SimplePythonFunction(
    command: Seq[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    private val _pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: PythonAccumulatorV2) extends PythonFunction {

  def this(
      command: Array[Byte],
      envVars: JMap[String, String],
      pythonIncludes: JList[String],
      _pythonExec: String,
      pythonVer: String,
      broadcastVars: JList[Broadcast[PythonBroadcast]],
      accumulator: PythonAccumulatorV2) = {
    this(command.toSeq, envVars, pythonIncludes, _pythonExec, pythonVer, broadcastVars, accumulator)
  }

  def pythonExec: String = {
    sys.env.getOrElse("PYSPARK_EXECUTOR_PYTHON", _pythonExec)
  }
}
