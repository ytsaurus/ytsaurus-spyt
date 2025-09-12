package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.resource.ResourceProfile


trait ExecutorAllocator {
  /*
    * Set the total expected executors for an application
    */
  def setTotalExpectedExecutors(resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Boolean
}
