package org.apache.spark.sql

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtReader, YtWriter}
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TmpDir}

class PartitioningTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with MockitoSugar
  with DynTableTestUtils with YtDistributedReadingTestUtils {
  import spark.implicits._

  private val conf = Map(
    WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
    CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb"
  )

  testWithDistributedReading("serialize partitioning") { _ =>
    withConfs(conf) {
      (0 to 1000).map(x => (x, "q")).toDF("a", "b").write.yt(tmpPath)
      spark.read.yt(tmpPath).createOrReplaceTempView("table")
      val df = spark.sql(s"SELECT /*+ BROADCAST(t2) */ t1.a FROM table t1 INNER JOIN table t2 ON t1.a = t2.a")
      // Without fix it throws java.io.NotSerializableException
      df.collect()

      val joinExec = df.queryExecution.executedPlan.collectFirst { case b: BroadcastHashJoinExec => b }
      // Double-check
      SparkEnv.get.closureSerializer.newInstance().serialize(joinExec.get.outputPartitioning)
    }
  }
}
