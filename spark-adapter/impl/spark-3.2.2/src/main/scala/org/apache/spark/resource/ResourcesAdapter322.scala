package org.apache.spark.resource

import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults
import tech.ytsaurus.spyt.{MinSparkVersion, ResourcesAdapter}

@MinSparkVersion("3.2.2")
class ResourcesAdapter322 extends ResourcesAdapter {

  override def getExecutorCores(execResources: Product): Int = {
    execResources.asInstanceOf[ExecutorResourcesOrDefaults].cores
  }
}
