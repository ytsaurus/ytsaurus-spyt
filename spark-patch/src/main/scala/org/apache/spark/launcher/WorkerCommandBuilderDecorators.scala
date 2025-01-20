package org.apache.spark.launcher

import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.util.{List => JList}

@Decorate
@OriginClass("org.apache.spark.launcher.WorkerCommandBuilder")
private[spark] class WorkerCommandBuilderDecorators {

  @DecoratedMethod
  def buildCommand(): JList[String] = {
    val cmd = __buildCommand()
    if (sys.env.contains("SPARK_JAVA_LOG4J_CONFIG")) {
      var javaPropertiesFirstPosition = 0
      while (!cmd.get(javaPropertiesFirstPosition).startsWith("-D")) {
        javaPropertiesFirstPosition += 1
      }

      cmd.add(javaPropertiesFirstPosition, sys.env("SPARK_JAVA_LOG4J_CONFIG"))
    }
    cmd
  }

  def __buildCommand(): JList[String] = ???

}
