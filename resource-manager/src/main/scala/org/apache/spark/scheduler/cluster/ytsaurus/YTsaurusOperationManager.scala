
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.deploy.ytsaurus.Config._
import org.apache.spark.deploy.ytsaurus.YTsaurusUtils.isShell
import org.apache.spark.deploy.ytsaurus.{ApplicationArguments, Config, YTsaurusUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{Utils, VersionUtils}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import tech.ytsaurus.client.YTsaurusClient
import tech.ytsaurus.client.operations.{Spec, VanillaSpec}
import tech.ytsaurus.client.request.{CompleteOperation, GetOperation, UpdateOperationParameters, VanillaOperation}
import tech.ytsaurus.client.rpc.YTsaurusClientAuth
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree._

import java.net.URI
import java.nio.file.Paths
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt


private[spark] class YTsaurusOperationManager(val ytClient: YTsaurusClient,
                                              user: String, token: String,
                                              portoLayers: YTreeNode,
                                              filePaths: YTreeNode,
                                              environment: YTreeMapNode,
                                              home: String,
                                              prepareEnvCommand: String,
                                              sparkClassPath: String,
                                              javaCommand: String,
                                              ytsaurusJavaOptions: Seq[String])
  extends Logging {

  import YTsaurusOperationManager._

  def startDriver(conf: SparkConf, appArgs: ApplicationArguments): YTsaurusOperation = {
    val opParams = driverParams(conf, appArgs)
    val operation = startVanillaOperation(conf, DRIVER_TASK, opParams)
    conf.set(Config.DRIVER_OPERATION_ID, operation.id.toString)
    logInfo(s"Driver operation ID: ${operation.id}")
    operation
  }

  def startExecutors(sc: SparkContext,
                     appId: String,
                     resourceProfile: ResourceProfile,
                     numExecutors: Int): YTsaurusOperation = {
    val opParams = executorParams(sc.conf, appId, resourceProfile, numExecutors)
    val operation = startVanillaOperation(sc.conf, EXECUTOR_TASK, opParams)
    // TODO 2. autoscaling with multiple operations
    sc.conf.set(Config.EXECUTOR_OPERATION_ID, operation.id.toString)
    logInfo(s"Executor operation ID: ${operation.id}")
    operation
  }

  def setOperationDescription(operation: YTsaurusOperation, description: Map[String, String]): Unit = {
    val yTreeDescription = description.foldLeft(YTree.mapBuilder()) { case (builder, (k, v)) =>
      builder.key(k).value(v)
    }.buildMap()
    val annotations = YTree.mapBuilder().key("description").value(yTreeDescription).buildMap()
    val req = UpdateOperationParameters
      .builder()
      .setOperationId(operation.id)
      .setAnnotations(annotations)
      .build()

    ytClient.updateOperationParameters(req)
  }

  def stopExecutors(sc: SparkContext): Unit = {
    sc.conf.getOption(Config.EXECUTOR_OPERATION_ID).foreach { opId =>
      val operation = YTsaurusOperation(GUID.valueOf(opId))
      if (!isFinalState(getOperationState(getOperation(operation, ytClient)))) {
        val completeRequest = CompleteOperation.builder().setOperationId(operation.id).build()
        ytClient.completeOperation(completeRequest).join()
      }
    }
  }

  def close(): Unit = {
    logInfo("Closing YTsaurus operation manager")
    ytClient.close()
  }

  private def startVanillaOperation(
                                     conf: SparkConf, taskName: String, opParams: OperationParameters): YTsaurusOperation = {
    val jobSpec = createSpec(conf, taskName, opParams)
    val operationSpec = VanillaOperation.builder().setSpec(jobSpec).build()
    val runningOperation = ytClient.startVanilla(operationSpec).get()
    val operationId = runningOperation.getId
    YTsaurusOperation(operationId)
  }

  private[ytsaurus] def createSpec(conf: SparkConf, taskName: String, opParams: OperationParameters): VanillaSpec = {
    val poolParameters = conf.get(YTSAURUS_POOL).map(pool => Map("pool" -> YTree.stringNode(pool))).getOrElse(Map.empty)

    val opSpecBuilder: VanillaSpec.BuilderBase[_] = VanillaSpec.builder()
    opSpecBuilder.setTask(taskName, opParams.taskSpec)

    val secureVault = YTree.mapBuilder()
      .key("YT_USER").value(user)
      .key("YT_TOKEN").value(token)
      .buildMap()

    val title = s"Spark $taskName for ${conf.get("spark.app.name")}${opParams.attemptId}"

    val userParameters = conf.getOption(s"spark.ytsaurus.$taskName.operation.parameters")
      .map(YTreeTextSerializer.deserialize(_).asMap().asScala).getOrElse(Map.empty)

    val annotations = SpecificationUtils.getAnnotationsAsYTreeMapNode(conf, taskName)
    val additionalParameters: Map[String, YTreeNode] = (userParameters ++ Map(
      "secure_vault" -> secureVault,
      "max_failed_job_count" -> YTree.integerNode(opParams.maxFailedJobCount),
      "preemption_mode" -> YTree.stringNode("normal"),
      "title" -> YTree.stringNode(title),
    ) ++ (if (!annotations.isEmpty) Map("annotations" -> annotations) else Map.empty) ++
      poolParameters).toMap

    opSpecBuilder.setAdditionalSpecParameters(additionalParameters.asJava)
    opSpecBuilder.build()
  }

  private def driverParams(conf: SparkConf, appArgs: ApplicationArguments): OperationParameters = {
    val driverMemoryMiB = conf.get(DRIVER_MEMORY)

    val sparkJavaOpts = Utils.sparkJavaOpts(conf).map { opt =>
      val Array(k, v) = opt.split("=", 2)
      k + "=\"" + v + "\""
    } ++ Seq(s"-D${Config.DRIVER_OPERATION_ID}=$$YT_OPERATION_ID")

    val driverOpts = conf.get(DRIVER_JAVA_OPTIONS).getOrElse("")

    val additionalArgs: Seq[String] = appArgs.mainAppResourceType match {
      case "python" =>
        val pythonFile = Paths.get(appArgs.mainAppResource.get).getFileName.toString
        val suppliedFiles = (conf.get(FILES) ++ conf.get(SUBMIT_PYTHON_FILES)).map { fileName =>
          s"$home/${Paths.get(fileName).getFileName.toString}"
        }
        val pyFiles = (Seq(s"$home/spyt-package/python") ++ suppliedFiles).mkString(",")
        Seq(pythonFile, pyFiles)
      case _ => Seq.empty
    }

    var driverCommand = (Seq(
      prepareEnvCommand,
      "&&",
      javaCommand,
      s"-Xmx${driverMemoryMiB}m",
      "-cp", sparkClassPath) ++
      sparkJavaOpts ++
      ytsaurusJavaOptions ++ Seq(
      driverOpts,
      "org.apache.spark.deploy.ytsaurus.DriverWrapper",
      appArgs.mainClass
    ) ++ additionalArgs ++ appArgs.driverArgs).mkString(" ")

    driverCommand = addRedirectToStderrIfNeeded(conf, driverCommand)

    val overheadFactor = if (appArgs.mainAppResourceType == "java") {
      MEMORY_OVERHEAD_FACTOR
    } else {
      NON_JVM_MEMORY_OVERHEAD_FACTOR
    }

    val memoryOverheadMiB = conf
      .get(DRIVER_MEMORY_OVERHEAD)
      .getOrElse(math.max((overheadFactor * driverMemoryMiB).toInt,
        ResourceProfile.MEMORY_OVERHEAD_MIN_MIB))

    val memoryLimit = (driverMemoryMiB + memoryOverheadMiB) * MIB

    conf.get(YTSAURUS_PYTHON_BINARY_ENTRY_POINT).foreach { ep =>
      environment.put("Y_PYTHON_ENTRY_POINT", YTree.stringNode(ep))
    }

    val spec: Spec = (specBuilder, _, _) => specBuilder.beginMap()
      .key("command").value(driverCommand)
      .key("job_count").value(1)
      .key("cpu_limit").value(conf.get(DRIVER_CORES))
      .key("memory_limit").value(memoryLimit)
      .key("layer_paths").value(portoLayers)
      .key("file_paths").value(filePaths)
      .key("environment").value(environment)
      .endMap()

    OperationParameters(spec, conf.get(MAX_DRIVER_FAILURES), "")
  }

  private def addRedirectToStderrIfNeeded(conf: SparkConf, cmd: String): String = {
    if (conf.get(YTSAURUS_REDIRECT_STDOUT_TO_STDERR)) {
      cmd + " 1>&2"
    } else {
      cmd
    }
  }


  private[ytsaurus] def executorParams(conf: SparkConf, appId: String, resourceProfile: ResourceProfile,
                                       numExecutors: Int): OperationParameters = {

    val isPythonApp = conf.get(YTSAURUS_IS_PYTHON)

    val driverUrl = RpcEndpointAddress(
      conf.get(DRIVER_HOST_ADDRESS),
      conf.getInt(DRIVER_PORT.key, DEFAULT_DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

    val execResources = ResourceProfile.getResourcesForClusterManager(
      resourceProfile.id,
      resourceProfile.executorResources,
      MEMORY_OVERHEAD_FACTOR,
      conf,
      isPythonApp,
      Map.empty)

    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf).map { opt =>
      val Array(k, v) = opt.split("=", 2)
      k + "=\"" + v + "\""
    }

    val executorOpts = conf.get(EXECUTOR_JAVA_OPTIONS).getOrElse("")

    if (isPythonApp && conf.get(YTSAURUS_PYTHON_EXECUTABLE).isDefined) {
      environment.put("PYSPARK_EXECUTOR_PYTHON", YTree.stringNode(conf.get(YTSAURUS_PYTHON_EXECUTABLE).get))
    }

    if (conf.get(YTSAURUS_IS_PYTHON_BINARY)) {
      val binaryExecutable = Paths.get(conf.get(SPARK_PRIMARY_RESOURCE)).getFileName.toString
      environment.put("Y_BINARY_EXECUTABLE", YTree.stringNode(binaryExecutable))
    }

    var executorCommand = (Seq(
      prepareEnvCommand,
      "&&",
      javaCommand,
      "-cp", sparkClassPath,
      s"-Xmx${execResources.executorMemoryMiB}m") ++
      // classpath
      sparkJavaOpts ++
      ytsaurusJavaOptions ++ Seq(
      executorOpts,
      "org.apache.spark.executor.YTsaurusCoarseGrainedExecutorBackend",
      "--driver-url", driverUrl,
      "--executor-id", "$YT_TASK_JOB_INDEX",
      "--cores", execResources.cores.toString,
      "--app-id", appId,
      "--hostname", "$HOSTNAME"
    )).mkString(" ")

    executorCommand = addRedirectToStderrIfNeeded(conf, executorCommand)

    val memoryLimit = execResources.totalMemMiB * MIB
    val gpuLimit = execResources.customResources.get("gpu").map(_.amount).getOrElse(0L)

    val spec: Spec = (specBuilder, _, _) => {
      specBuilder.beginMap()
        .key("command").value(executorCommand)
        .key("job_count").value(numExecutors)
        .key("cpu_limit").value(execResources.cores)
        .key("memory_limit").value(memoryLimit)
        .key("layer_paths").value(portoLayers)
        .key("file_paths").value(filePaths)
        .key("environment").value(environment)
      if (gpuLimit > 0) {
        specBuilder
          .key("gpu_limit").value(gpuLimit)
          .key("cuda_toolkit_version").value(conf.get(YTSAURUS_CUDA_VERSION).get)
      }
      specBuilder.endMap()
    }

    val attemptId = s" [${sys.env.getOrElse("YT_TASK_JOB_INDEX", "0")}]"
    OperationParameters(spec, conf.get(MAX_EXECUTOR_FAILURES) * numExecutors, attemptId)
  }
}

private[spark] object YTsaurusOperationManager extends Logging {

  def create(ytProxy: String, conf: SparkConf, networkName: Option[String], proxyRole: Option[String]): YTsaurusOperationManager = {
    val (user, token) = YTsaurusUtils.userAndToken(conf)
    val ytClient: YTsaurusClient = buildClient(ytProxy, user, token, networkName, proxyRole)

    try {
      val globalConfigPath = conf.get(GLOBAL_CONFIG_PATH)
      val globalConfig: YTreeMapNode = getDocument(ytClient, globalConfigPath)

      val spytVersion = conf.get(SPYT_VERSION).getOrElse(getLatestRelease(ytClient, conf))
      logInfo(s"Used SPYT version: $spytVersion")
      val environment = globalConfig.getMap("environment")
      val javaHome = environment.getStringO("JAVA_HOME")
        .orElseThrow(() => new SparkException(s"JAVA_HOME is not set in ${GLOBAL_CONFIG_PATH.key} parameter value"))

      val releaseConfigPath = s"${conf.get(RELEASE_CONFIG_PATH)}/$spytVersion/${conf.get(LAUNCH_CONF_FILE)}"

      val releaseConfig: YTreeMapNode = getDocument(ytClient, releaseConfigPath)

      val portoLayers = getPortoLayers(conf, releaseConfig.getListO("layer_paths").orElse(YTree.listBuilder().buildList()))

      val sv = org.apache.spark.SPARK_VERSION_SHORT
      val (svMajor, svMinor, svPatch) = VersionUtils.majorMinorPatchVersion(sv).get
      val distrRootPath = Seq(conf.get(SPARK_DISTRIBUTIVES_PATH), svMajor, svMinor, svPatch).mkString("/")
      val distrTgzOpt = Some(distrRootPath).filter(path => ytClient.existsNode(path).join()).flatMap { path =>
        val distrRootContents = ytClient.listNode(path).join().asList()
        distrRootContents.asScala.find(_.stringValue().endsWith(".tgz"))
      }

      val filePaths = if (conf.contains(DRIVER_OPERATION_ID)) {
        val driverOpId = conf.get(DRIVER_OPERATION_ID)
        val driverOperation = getOperation(YTsaurusOperation(GUID.valueOf(driverOpId)), ytClient)
        getDriverFilePaths(driverOperation)
      } else {
        val filePathsList = releaseConfig.getListO("file_paths").orElse(YTree.listBuilder().buildList())

        applicationFiles(conf, localFileToCacheUploader(conf, ytClient)).foreach { appFile =>
          val node = YTree.stringNode(appFile.ytPath)
          node.putAttribute("file_name", YTree.stringNode(appFile.downloadName))
          if (appFile.isExecutable) {
            node.putAttribute("executable", YTree.booleanNode(true))
          }
          filePathsList.add(node)
        }

        distrTgzOpt match {
          case Some(sparkTgz) => filePathsList.add(YTree.stringNode(s"$distrRootPath/${sparkTgz.stringValue()}"))
          case _ => throw new SparkException(s"Spark $sv tgz distributive doesn't exist " +
            s"at path $distrRootPath on cluster $ytProxy")
        }

        filePathsList
      }

      enrichSparkConf(conf, releaseConfig)
      enrichSparkConf(conf, globalConfig)

      val javaCommand = s"$javaHome/bin/java"
      val home = "."
      val sparkHome = s"$home/spark"
      val spytHome = s"$home/spyt-package"
      val sparkClassPath = s"$home/*:$spytHome/conf/:$spytHome/jars/*:$sparkHome/jars/*"
      environment.put("SPARK_HOME", YTree.stringNode(sparkHome))

      if (conf.get(YTSAURUS_IS_PYTHON_BINARY)) {
        val pyBinaryWrapper = YTree.stringNode(YTsaurusUtils.pythonBinaryWrapperPath(spytHome))
        environment.put("PYSPARK_EXECUTOR_PYTHON", pyBinaryWrapper)
        if (conf.get(SUBMIT_DEPLOY_MODE) == "cluster") {
          environment.put("PYSPARK_DRIVER_PYTHON", pyBinaryWrapper)
        }
      } else {
        environment.put("PYTHONPATH", YTree.stringNode(s"$spytHome/python"))
      }

      conf.set("spark.executor.resource.gpu.discoveryScript", s"$spytHome/bin/getGpusResources.sh")

      val ytsaurusJavaOptions = ArrayBuffer[String]()
      if (conf.getBoolean("spark.hadoop.yt.preferenceIpv6.enabled", defaultValue = false)) {
        ytsaurusJavaOptions += "-Djava.net.preferIPv6Addresses=true"
      }
      ytsaurusJavaOptions += s"$$(cat $spytHome/conf/java-opts)"

      if (!conf.contains(YTSAURUS_CUDA_VERSION)) {
        val cudaVersion = globalConfig.getStringO("cuda_toolkit_version").orElse("11.0")
        conf.set(YTSAURUS_CUDA_VERSION, cudaVersion)
      }

      val sparkTgz = distrTgzOpt.get.stringValue()
      val prepareEnvCommand = s"./setup-spyt-env.sh --spark-home $home --spark-distributive $sparkTgz"

      new YTsaurusOperationManager(
        ytClient,
        user,
        token,
        portoLayers,
        filePaths,
        environment,
        home,
        prepareEnvCommand,
        sparkClassPath,
        javaCommand,
        ytsaurusJavaOptions
      )
    } catch {
      case t: Throwable =>
        ytClient.close()
        throw t
    }
  }

  def getOperation(operation: YTsaurusOperation, ytClient: YTsaurusClient): YTreeNode = {
    val request = GetOperation.builder().setOperationId(operation.id).build()
    ytClient.getOperation(request).join()
  }

  private[ytsaurus] def getPortoLayers(conf: SparkConf, defaultLayers: YTreeListNode): YTreeNode = {
    val portoLayers = YTree.listBuilder().buildList()

    conf.get(YTSAURUS_EXTRA_PORTO_LAYER_PATHS).foreach(extraPortoLayers => {
      extraPortoLayers.split(',').foreach(layer => {
        portoLayers.add(YTree.stringNode(layer))
      })
    })

    if (conf.contains(YTSAURUS_PORTO_LAYER_PATHS)){
      conf.get(YTSAURUS_PORTO_LAYER_PATHS).foreach(layers => {
        layers.split(',').foreach(layer => {
          portoLayers.add(YTree.stringNode(layer))
        })
      })
    } else {
      defaultLayers.forEach(layer => {
        portoLayers.add(YTree.stringNode(layer.stringValue()))
      })
    }

    portoLayers
  }

  private[ytsaurus] def getDocument(ytClient: YTsaurusClient, path: String): YTreeMapNode = {
    if (ytClient.existsNode(path).join()) {
      ytClient.getNode(path).join().mapNode()
    } else {
      logWarning(s"Document at path $path does not exist")
      YTree.mapBuilder().buildMap()
    }
  }

  private def parseVersion(version: String): SpytVersion = {
    val array = version.split("\\.").map(_.toInt)
    require(array.length == 3, s"Release version ($version) must have 3 numbers")
    SpytVersion(array(0), array(1), array(2))
  }

  private[ytsaurus] def getLatestRelease(ytClient: YTsaurusClient, conf: SparkConf): String = {
    val path = conf.get(RELEASE_SPYT_PATH)
    val releaseNodes = ytClient.listNode(path).join().asList().asScala
    if (releaseNodes.isEmpty) {
      throw new IllegalStateException(s"No releases found in $path")
    }
    val versions = releaseNodes.map(x => parseVersion(x.stringValue()))
    versions.max.toString
  }

  private[ytsaurus] case class ApplicationFile(ytPath: String,
                                               targetName: Option[String] = None,
                                               isArchive: Boolean = false,
                                               isExecutable: Boolean = false) {
    private def originName: String = ytPath.split('/').last

    private def originExtension: String = originName.split("\\.", 2).last

    private def localName: String = targetName.getOrElse(originName)

    def downloadName: String = if (isArchive) s"$localName-arc-dep.$originExtension" else localName
  }

  type UploadToCache = String => String

  def localFileToCacheUploader(conf: SparkConf, ytClient: YTsaurusClient): UploadToCache = (path: String) => {
    val remoteTempFilesDirectory = conf.get(Config.YTSAURUS_REMOTE_TEMP_FILES_DIRECTORY)
    YtWrapper.uploadFileToCache(path, 7.days, remoteTempFilesDirectory)(ytClient)
  }

  private[ytsaurus] def extractYtFiles(files: Seq[String],
                                       uploadToCache: UploadToCache,
                                       isArchive: Boolean = false): Seq[ApplicationFile] = {
    files.map(fileName => prepareFile(fileName, uploadToCache, isArchive))
  }

  private def prepareFile(fileName: String,
                          uploadToCache: UploadToCache,
                          isArchive: Boolean): ApplicationFile = {
    val parts = fileName.split('#')
    val (sourceName, specifiedName) = parts.length match {
      case 1 => (fileName, None)
      case 2 => (parts.head, Some(parts.last))
      case _ => throw new SparkException(s"Too many '#': $fileName")
    }

    val (ytFileName, targetName) = if (sourceName.startsWith("yt:/")) {
      (YtWrapper.formatPath(sourceName), specifiedName)
    } else {
      val uri = URI.create(sourceName)
      (uploadToCache(uri.getPath), specifiedName.orElse(Some(Paths.get(uri.getPath).getFileName.toString)))
    }
    ApplicationFile(ytFileName, targetName, isArchive)
  }

  // This value is based on these code in Spark:
  // https://github.com/apache/spark/blob/v3.5.3/core/src/main/scala/org/apache/spark/util/Utils.scala#L238
  // https://github.com/apache/spark/blob/v3.5.3/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L372
  // It is used to exclude caching local files on YTsaurus that were previosly downloaded from cypress when
  // running Spark in client mode
  private val SPARK_TMP_DIR_PREFIX = s"${System.getProperty("java.io.tmpdir")}/spark"

  private[ytsaurus] def applicationFiles(conf: SparkConf, uploadToCache: UploadToCache): Seq[ApplicationFile] = {
    val files = Seq(JARS, FILES, ARCHIVES, SUBMIT_PYTHON_FILES)
      .flatMap(k =>
        extractYtFiles(conf.get(k).filter(res => !res.startsWith(SPARK_TMP_DIR_PREFIX)), uploadToCache, k == ARCHIVES)
      )
    val primaryResource =
      extractYtFiles(
        Option(conf.get(SPARK_PRIMARY_RESOURCE)).filter(res => res != SparkLauncher.NO_RESOURCE && !isShell(res)).toSeq,
        uploadToCache
      ).map(_.copy(isExecutable = conf.get(YTSAURUS_IS_PYTHON_BINARY)))

    (files ++ primaryResource).distinct
  }

  private[ytsaurus] def enrichSparkConf(conf: SparkConf, ytSparkConfig: YTreeMapNode): Unit = {
    if (ytSparkConfig.containsKey("spark_conf")) {
      ytSparkConfig.getMap("spark_conf").asScala.foreach { entry =>
        if (!conf.contains(entry.getKey)) {
          conf.set(entry.getKey, entry.getValue.stringNode().getValue)
        }
      }
    }

    if (ytSparkConfig.containsKey("enablers")) {
      val enablers = ytSparkConfig.getMap("enablers")
      enablers.keys().forEach { enabler =>
        if (conf.contains(enabler)) {
          val confValue = conf.getBoolean(enabler, defaultValue = false)
          val updatedValue = confValue && enablers.getBool(enabler)
          if (confValue != updatedValue) {
            logWarning(s"Property $enabler was explicitly set to $updatedValue because of cluster settings")
            conf.set(enabler, updatedValue.toString)
          }
        }
      }
    }
  }

  val MEMORY_OVERHEAD_FACTOR = 0.1
  val NON_JVM_MEMORY_OVERHEAD_FACTOR = 0.4
  val DEFAULT_DRIVER_PORT = 27001
  val MIB: Long = 1L << 20

  val DRIVER_TASK = "driver"
  val EXECUTOR_TASK = "executor"

  private val finalStates = Set("completed", "failed", "aborted", "lost")

  val WEB_UI_KEY = "Web UI"
  private val WEB_UI_PATH = List("runtime_parameters", "annotations", "description", WEB_UI_KEY)

  def getOperationState(operation: YTreeNode): String = {
    operation.mapNode().getStringO("state").orElse("undefined")
  }

  private def getDriverFilePaths(operation: YTreeNode): YTreeNode = {
    operation.mapNode().getMap("provided_spec").getMap("tasks").getMap("driver").getList("file_paths")
  }

  def isCompletedState(currentState: String): Boolean = currentState == "completed"

  def isFinalState(currentState: String): Boolean = {
    finalStates.contains(currentState)
  }

  def getWebUIAddress(operation: YTreeNode): Option[String] = {
    val description = WEB_UI_PATH.foldLeft(Some(operation).asInstanceOf[Option[YTreeNode]]) { (yTreeOpt, key) =>
      yTreeOpt.map(_.asMap()).filter(_.containsKey(key)).map(_.get(key))
    }

    description.filter(_.isStringNode).map(_.stringNode().getValue)
  }

  private def buildClient(ytProxy: String,
                          user: String,
                          token: String,
                          networkName: Option[String],
                          proxyRole: Option[String]): YTsaurusClient = {
    val builder: YTsaurusClient.ClientBuilder[_ <: YTsaurusClient, _] = YTsaurusClient.builder()
    builder.setCluster(ytProxy)
    networkName.foreach(nn => builder.setProxyNetworkName(nn))
    proxyRole.foreach(pr => builder.setProxyRole(pr))
    if (user != null && token != null) {
      builder.setAuth(YTsaurusClientAuth.builder().setUser(user).setToken(token).build())
    }
    builder.build()
  }
}

private[spark] case class YTsaurusOperation(id: GUID)

private[spark] case class OperationParameters(taskSpec: Spec, maxFailedJobCount: Int, attemptId: String)

private case class SpytVersion(major: Int, minor: Int, patch: Int) extends Comparable[SpytVersion] {
  override def compareTo(o: SpytVersion): Int = {
    100 * (major - o.major).signum + 10 * (minor - o.minor).signum + (patch - o.patch).signum
  }

  override def toString: String = s"$major.$minor.$patch"
}
