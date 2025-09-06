package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleManager}
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf, SparkContext, TaskContext}
import org.mockito.Mockito
import org.mockito.scalatest.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test.LocalYt

class YTsaurusShuffleManagerTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with MockitoSugar {
  behavior of "YTsaurusShuffleManager"

  private var sc: SparkContext = _
  private var shuffleManager: ShuffleManager = _
  private var handle: ShuffleHandle = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    shuffleManager = null
    handle = null
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (sc != null) {
      sc.stop()
    }
    if (shuffleManager != null && handle != null) {
      shuffleManager.unregisterShuffle(handle.shuffleId)
    }
  }

  private def createTaskContextStub(partitionId: Int): TaskContext = {
    val taskContext = mock[TaskContext](Mockito.withSettings().lenient())
    val metrics = TaskMetrics.empty
    when(taskContext.taskMetrics()).thenReturn(metrics)
    when(taskContext.partitionId()).thenReturn(partitionId)
    Mockito.doNothing().when(taskContext).killTaskIfInterrupted()
    taskContext
  }

  private def testTemplate(
    shuffleId: Int,
    nMapTasks: Int,
    nOutputPartitions: Int,
    extraConf: Map[String, String] = Map.empty,
    skipOutputPartitions: Set[Int] = Set.empty
  ): Unit = {
    val conf = new SparkConf()
    conf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager")
      .set("spark.shuffle.sort.io.plugin.class", "tech.ytsaurus.spyt.shuffle.YTsaurusShuffleDataIO")
      .set("spark.hadoop.yt.proxy", LocalYt.proxy)
      .set("spark.hadoop.yt.user", "root")
      .set("spark.hadoop.yt.token", "")
      .set("spark.ytsaurus.shuffle.replication.factor", "1")

    extraConf.foreach { case (key, value) => conf.set(key, value) }

    sc = new SparkContext("local", "test", conf)
    val shuffleManager = sc.env.shuffleManager

    shuffleManager shouldBe a[YTsaurusShuffleManager]

    val shuffleMapRdd = sc.parallelize(1 to 100).map(x => x -> s"Value $x")
    val shuffleDependency = new ShuffleDependency(shuffleMapRdd, new IntPartitioner(nOutputPartitions))
    handle = shuffleManager.registerShuffle(shuffleId, shuffleDependency)

    (0 until nMapTasks).foreach { mapId =>
      val taskContext = createTaskContextStub(mapId)
      val metrics = taskContext.taskMetrics.shuffleWriteMetrics
      val dataToWrite = (0 until (nOutputPartitions*3))
        .map(pId => (pId / 3) -> s"M$mapId P$pId")
        .filter(row => !skipOutputPartitions.contains(row._1))

      val writer = shuffleManager.getWriter[Int, String](handle, mapId, taskContext, metrics)
      writer.write(dataToWrite.iterator)
      writer.stop(success = true)
    }

    (0 until nOutputPartitions).foreach { reduceId =>
      val taskContext = createTaskContextStub(reduceId)
      val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
      val reader = shuffleManager.getReader[Int, String](handle, reduceId, reduceId + 1, taskContext, metrics)
      val result = reader.read().toList
      val expected = if (skipOutputPartitions.contains(reduceId)) Nil else (0 until nMapTasks).flatMap { mapId =>
        (0 until 3).map(itemId => reduceId -> s"M$mapId P${reduceId*3 + itemId}")
      }

      result should contain theSameElementsAs expected
    }

  }

  it should "write and read shuffle data" in {
    testTemplate(1, 4, 10)
  }

  it should "read empty shuffle partitions without compression" in {
    testTemplate(1, 4, 10, Map("spark.shuffle.compress"-> "false"), skipOutputPartitions = Set(3,5))
  }

  it should "read empty shuffle partitions with compression" in {
    testTemplate(1, 4, 10, Map("spark.shuffle.compress"-> "true"), skipOutputPartitions = Set(3,5))
  }
}

class IntPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
