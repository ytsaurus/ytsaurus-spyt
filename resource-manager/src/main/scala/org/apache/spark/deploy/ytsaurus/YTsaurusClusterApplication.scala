
package org.apache.spark.deploy.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.ytsaurus.Config.{SUBMISSION_ID, YTSAURUS_DRIVER_OPERATION_DUMP_PATH, YTSAURUS_DRIVER_WATCH}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager.{getOperation, getOperationState, getWebUIAddress, isCompletedState}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Promise
import scala.reflect.io.File


private[spark] class YTsaurusClusterApplication extends SparkApplication with Logging {
  import YTsaurusClusterApplication._

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val masterURL = conf.get("spark.master")
    val ytProxy = YTsaurusUtils.parseMasterUrl(masterURL)
    val proxyRole = conf.getOption("spark.hadoop.yt.proxyRole")
    val networkName = conf.getOption("spark.hadoop.yt.proxyNetworkName")
    if (conf.contains("spark.hadoop.yt.clusterProxy")) {
      conf.set("spark.hadoop.yt.proxy", conf.get("spark.hadoop.yt.clusterProxy"))
    }
    if (conf.contains("spark.hadoop.yt.clusterProxyRole")) {
      conf.set("spark.hadoop.yt.proxyRole", conf.get("spark.hadoop.yt.clusterProxyRole"))
    }

    val appArgs = ApplicationArguments.fromCommandLineArgs(args)

    logInfo(s"Submitting spark application to YTsaurus cluster at $ytProxy")
    logInfo(s"Application arguments: $appArgs")

    val operationManager = YTsaurusOperationManager.create(ytProxy, conf, networkName, proxyRole)
    val submissionIdOpt = conf.getOption(SUBMISSION_ID)

    try {
      val driverOperation = operationManager.startDriver(conf, appArgs)
      val operationId = driverOperation.id.toString
      conf.get(YTSAURUS_DRIVER_OPERATION_DUMP_PATH).foreach { File(_).writeAll(operationId) }
      if (conf.get(YTSAURUS_DRIVER_WATCH)) {
        var currentState = "undefined"
        var webUIAddress: Option[String] = None
        while (!YTsaurusOperationManager.isFinalState(currentState)) {
          Thread.sleep(pingInterval)
          val opSpec = getOperation(driverOperation, operationManager.ytClient)
          currentState = getOperationState(opSpec)
          logInfo(s"Operation: $operationId, State: $currentState")

          val currentWebUiAddress = getWebUIAddress(opSpec)
          if (currentWebUiAddress.isDefined && currentWebUiAddress != webUIAddress) {
            webUIAddress = currentWebUiAddress
            webUIAddress.foreach(addr => logInfo(s"Web UI: $addr"))
          }
        }
        if (!isCompletedState(currentState)) {
          throw new IllegalStateException(s"Unsuccessful operation state: $currentState")  // For non-zero exit code
        }
      } else {
        YTsaurusClusterApplication.completeOperationId(submissionIdOpt, operationId)
        logInfo(s"Driver operation: $operationId")
      }
    } catch {
      case e: Exception =>
        YTsaurusClusterApplication.failOperationId(submissionIdOpt, e)
        throw e
    } finally {
      operationManager.close()
    }
  }
}

object YTsaurusClusterApplication {
  private val pingInterval = 3000L
  private val operationPromises = TrieMap.empty[String, Promise[String]]

  def getOrCreatePromise(submissionId: String): Promise[String] =
    operationPromises.getOrElseUpdate(submissionId, Promise[String]())

  def failOperationId(submissionId: String, ex: Throwable): Unit =
    operationPromises.remove(submissionId).foreach(_.tryFailure(ex))

  private def completeOperationId(submissionIdOpt: Option[String], operationId: String): Unit =
    submissionIdOpt.foreach { submissionId =>
      operationPromises.remove(submissionId).foreach(_.trySuccess(operationId))
    }

  private def failOperationId(submissionIdOpt: Option[String], ex: Throwable): Unit =
    submissionIdOpt.foreach(failOperationId(_, ex))
}

private[spark] case class ApplicationArguments(
  mainAppResource: Option[String],
  mainAppResourceType: String,
  mainClass: String,
  driverArgs: Array[String],
  proxyUser: Option[String])

private[spark] object ApplicationArguments {

  def fromCommandLineArgs(args: Array[String]): ApplicationArguments = {
    var mainAppResource: Option[String] = None
    var mainAppResourceType: String = "java"
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String] = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = Some(primaryJavaResource)
        mainAppResourceType = "java"
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = Some(primaryPythonResource)
        mainAppResourceType = "python"
      case Array("--primary-r-file", primaryRFile: String) =>
        mainAppResource = Some(primaryRFile)
        mainAppResourceType = "R"
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case Array("--proxy-user", user: String) =>
        proxyUser = Some(user)
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ApplicationArguments(
      mainAppResource,
      mainAppResourceType,
      mainClass.get,
      driverArgs.toArray,
      proxyUser)
  }
}
