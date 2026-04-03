package org.apache.spark.sql

import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults

// The sole purpose of this object is to increase visibility of some Spark package-private methods
object AdapterSupport340 {
  def getExecutorCores(execResources: Product): Int = {
    execResources.asInstanceOf[ExecutorResourcesOrDefaults].cores.getOrElse(1)
  }
}
