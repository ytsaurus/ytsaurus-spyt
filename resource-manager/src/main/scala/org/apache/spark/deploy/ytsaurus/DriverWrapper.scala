package org.apache.spark.deploy.ytsaurus

import org.apache.spark.internal.Logging
import org.apache.spark.util._

/**
 * Almost copy of org.apache.spark.deploy.worker.DriverWrapper
 */
object DriverWrapper extends Logging {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case mainClass :: extraArgs =>
        val currentLoader = Thread.currentThread.getContextClassLoader
        val loader = new MutableURLClassLoader(Array(), currentLoader) // Mutable classloader instead of system
        Thread.currentThread.setContextClassLoader(loader)

        val clazz = Utils.classForName(mainClass)
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, extraArgs.toArray[String])
      case _ =>
        System.err.println("Usage: DriverWrapper <driverMainClass> [options]")
        System.exit(-1)
    }
  }
}
