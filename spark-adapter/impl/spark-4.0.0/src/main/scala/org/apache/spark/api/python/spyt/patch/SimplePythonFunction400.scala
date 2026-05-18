package org.apache.spark.api.python.spyt.patch

import org.apache.spark.api.python.{PythonBroadcast, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.CollectionAccumulator
import tech.ytsaurus.spyt.patch.annotations.{Applicability, OriginClass}

import java.util.{List => JList, Map => JMap}

@OriginClass("org.apache.spark.api.python.SimplePythonFunction")
@Applicability(from = "4.0.0")
private[spark] case class SimplePythonFunction400(
  command: Seq[Byte],
  envVars: JMap[String, String],
  pythonIncludes: JList[String],
  private val _pythonExec: String,
  pythonVer: String,
  broadcastVars: JList[Broadcast[PythonBroadcast]],
  accumulator: CollectionAccumulator[Array[Byte]]) extends PythonFunction {

  def this(
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    _pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: CollectionAccumulator[Array[Byte]]) = {
    this(command.toSeq, envVars, pythonIncludes, _pythonExec, pythonVer, broadcastVars, accumulator)
  }

  def pythonExec: String = {
    sys.env.getOrElse("PYSPARK_EXECUTOR_PYTHON", _pythonExec)
  }
}
