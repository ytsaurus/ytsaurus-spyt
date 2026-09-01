
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.deploy.ytsaurus.Config.{EXECUTOR_OPERATION_SHUTDOWN_DELAY, YTSAURUS_EXECUTOR_APP_ID_CHECK_ENABLED}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager.EXECUTOR_APP_ID_ATTRIBUTE
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.scalatest.matchers.should.Matchers

class YTsaurusSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext with Matchers {

  private def createBackend(appIdCheckEnabled: Boolean = true): YTsaurusSchedulerBackend = {
    val conf = new SparkConf().setMaster("local").setAppName("YTsaurusSchedulerBackendSuite")
      .set(EXECUTOR_OPERATION_SHUTDOWN_DELAY.key, "0")
      .set(YTSAURUS_EXECUTOR_APP_ID_CHECK_ENABLED, appIdCheckEnabled)
    sc = new SparkContext(conf)
    new YTsaurusSchedulerBackend(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl], sc, YTsaurusOperationManagerStub())
  }

  private def registerExecutor(backend: YTsaurusSchedulerBackend, attributes: Map[String, String]): Boolean = {
    val executorRef = sc.env.rpcEnv.setupEndpoint("fake-executor", new RpcEndpoint {
      override val rpcEnv = sc.env.rpcEnv
      override def receive: PartialFunction[Any, Unit] = { case _ => }
    })
    backend.driverEndpoint.askSync[Boolean](RegisterExecutor("1", executorRef, "localhost", 1, Map.empty,
      attributes, Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
  }

  private def registrationError(backend: YTsaurusSchedulerBackend, attributes: Map[String, String]): String = {
    val e = intercept[Exception](registerExecutor(backend, attributes))
    Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" <- ")
  }

  test("Driver should accept an executor launched for this application") {
    val backend = createBackend()
    registerExecutor(backend, Map(EXECUTOR_APP_ID_ATTRIBUTE -> backend.applicationId())) shouldBe true
  }

  test("Driver should reject an executor launched for another application") {
    val backend = createBackend()
    val error = registrationError(backend, Map(EXECUTOR_APP_ID_ATTRIBUTE -> "spark-application-1"))
    error should include("spark-application-1")
    error should include(backend.applicationId())
  }

  test("Driver should reject an executor without application id") {
    val backend = createBackend()
    val error = registrationError(backend, Map.empty)
    error should include(backend.applicationId())
    error should include("<unknown>")
  }

  test("Application id check can be disabled") {
    val backend = createBackend(appIdCheckEnabled = false)
    registerExecutor(backend, Map(EXECUTOR_APP_ID_ATTRIBUTE -> "spark-application-1")) shouldBe true
  }
}
