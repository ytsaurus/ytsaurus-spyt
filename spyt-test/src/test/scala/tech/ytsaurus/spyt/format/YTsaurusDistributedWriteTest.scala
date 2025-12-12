package tech.ytsaurus.spyt.format

import org.apache.spark.SparkConf
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{ExecutorKillerSparkListener, LocalSpark, TestUtils, TmpDir}

class YTsaurusDistributedWriteTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TimeLimits
  with TestUtils {
  behavior of "DistributedWriteOutputCommitProtocol"

  override def sparkMaster: String = "local-cluster[3, 2, 1024]"

  override def reinstantiateSparkSession: Boolean = true

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "30")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "false")

      .set("spark.yt.write.distributed.enabled", "true")
  }

  implicit val defaultSignaler: Signaler = ThreadSignaler

  Seq(
    2 -> "at the beginning",
    20 -> "at the end"
  ).foreach { case (taskToKill, description) =>
    it should s"deal with final stage retries when stage fails $description" in withSparkSession() { _spark =>
      failAfter(120 seconds) {
        val df1 = _spark.range(1, 10000, 2)
        val df2 = _spark.range(1, 10000, 3)

        ExecutorKillerSparkListener.scheduleExecutorKillByTask(_spark) { taskSubmitted =>
          taskSubmitted.taskInfo.index == taskToKill
        }

        val result = df1.join(df2, Seq("id"), "outer").select(df1("id").alias("id_1"), df2("id").alias("id_2"))

        result.write.yt(tmpPath)
      }

      val writtenData = readTableAsYson(tmpPath).map { node =>
        val nodeMap = node.asMap()
        val id1Node = nodeMap.get("id_1")
        val id2Node = nodeMap.get("id_2")
        (
          if (id1Node.isEntityNode) null else id1Node.longValue(),
          if (id2Node.isEntityNode) null else id2Node.longValue()
        )
      }
      val expectedData = (1L until 10000L).filter(n => n % 2 == 1 || n % 3 == 1).map { n =>
        (if (n % 2 != 1) null else n, if (n % 3 != 1) null else n)
      }

      writtenData should contain theSameElementsAs expectedData
    }
  }
}
