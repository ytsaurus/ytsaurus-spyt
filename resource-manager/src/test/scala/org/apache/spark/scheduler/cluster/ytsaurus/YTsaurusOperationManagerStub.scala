
package org.apache.spark.scheduler.cluster.ytsaurus

import tech.ytsaurus.ysontree.YTree

object YTsaurusOperationManagerStub {
  val sparkClassPath: String =
    "$HOME/*:/usr/lib/spyt/conf/:/usr/lib/spyt/jars/scala-2.13/*:/usr/lib/spyt/jars/common/*:/usr/lib/spark/jars/*"

  def apply(): YTsaurusOperationManager = new YTsaurusOperationManager(
    ytClient = null,
    token = "testToken",
    layerPaths = YTree.listBuilder().buildList(),
    filePaths = YTree.listBuilder().buildList(),
    environment = YTree.mapBuilder().buildMap(),
    prepareEnvCommand = "./setup-spyt-env.sh --some-key some-value",
    sparkClassPath = sparkClassPath,
    javaCommand = "/usr/bin/java",
    ytsaurusJavaOptionsBash = ""
  )
}
