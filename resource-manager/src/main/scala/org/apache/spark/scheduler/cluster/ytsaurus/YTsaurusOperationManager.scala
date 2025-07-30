
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
import tech.ytsaurus.client.{YTsaurusClient, YTsaurusClientConfig}
import tech.ytsaurus.client.operations.{Spec, VanillaSpec}
import tech.ytsaurus.client.request.{CompleteOperation, GetOperation, UpdateOperationParameters, VanillaOperation}
import tech.ytsaurus.client.rpc.{RpcOptions, YTsaurusClientAuth}
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.{BuildInfo, SparkAdapter, SparkVersionUtils}
import tech.ytsaurus.spyt.wrapper.{YtWrapper, Utils => YtUtils}
import tech.ytsaurus.spyt.wrapper.Utils.{bashCommand, ytHostIpBashInlineWrapper}
import tech.ytsaurus.ysontree._

import java.net.URI
import java.nio.file.Paths
import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt


private[spark] class YTsaurusOperationManager(val ytClient: YTsaurusClient,
                                              user: String, token: String,
                                              layerPaths: YTreeNode,
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
    logDebug(s"[AppId: $appId] Requesting executors")
    val opParams = executorParams(sc.conf, appId, resourceProfile, numExecutors)
    logDebug(s"[AppId: $appId] Executor parameters ")
    val operation = startVanillaOperation(sc.conf, EXECUTOR_TASK, opParams)
    logDebug(s"[AppId: $appId] Vanilla operation has been successfully started")
    // TODO 2. autoscaling with multiple operations
    sc.conf.set(Config.EXECUTOR_OPERATION_ID, operation.id.toString)
    logInfo(s"[AppId: $appId] Executor operation ID: ${operation.id}")
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
    val opSpecBuilder: VanillaSpec.BuilderBase[_] = VanillaSpec.builder()
    opSpecBuilder.setTask(taskName, opParams.taskSpec)

    val additionalSpecParameters = conf.getOption(s"spark.ytsaurus.$taskName.operation.parameters")
      .map(YTreeTextSerializer.deserialize(_).asMap().asScala).getOrElse(mutable.HashMap[String, YTreeNode]())

    val secureVault = additionalSpecParameters.getOrElseUpdate("secure_vault", YTree.mapBuilder().buildMap())
    if (secureVault.isMapNode) {
      val map = secureVault.mapNode().asMap()
      map.putIfAbsent("YT_USER", YTree.stringNode(user))
      map.putIfAbsent("YT_TOKEN", YTree.stringNode(token))
    }
    additionalSpecParameters.getOrElseUpdate("max_failed_job_count", YTree.integerNode(opParams.maxFailedJobCount))
    additionalSpecParameters.getOrElseUpdate("preemption_mode", YTree.stringNode("normal"))
    additionalSpecParameters.getOrElseUpdate(
      "title", YTree.stringNode(s"Spark $taskName for ${conf.get("spark.app.name")}${opParams.attemptId}")
    )

    val annotations = SpecificationUtils.getAnnotationsAsYTreeMapNode(conf, taskName)
    if (!annotations.isEmpty) {
      val node = additionalSpecParameters.getOrElseUpdate("annotations", YTree.mapBuilder().buildMap())
      if (node.isMapNode) {
        annotations.asMap().forEach(node.mapNode().asMap().putIfAbsent)
      }
    }
    conf.get(YTSAURUS_POOL).foreach(pool => additionalSpecParameters.getOrElseUpdate("pool", YTree.stringNode(pool)))
    opSpecBuilder.setAdditionalSpecParameters(additionalSpecParameters.asJava)
    opSpecBuilder.build()
  }

  private[ytsaurus] def driverParams(conf: SparkConf, appArgs: ApplicationArguments): OperationParameters = {
    val driverMemoryMiB = conf.get(DRIVER_MEMORY)

    val netOptBash = conf.get(YTSAURUS_NETWORK_PROJECT)
      .map(_ => s""""-D${DRIVER_HOST_ADDRESS.key}=${ytHostIpBashInlineWrapper("YT_IP_ADDRESS_DEFAULT")}"""")
      .getOrElse("")

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

    var driverCommand = (
      s"$prepareEnvCommand && ${bashCommand(javaCommand, s"-Xmx${driverMemoryMiB}m", "-cp", sparkClassPath)}"
        + s" ${bashCommand(Utils.sparkJavaOpts(conf): _*)}"
        + s""" "-D${Config.DRIVER_OPERATION_ID}=$$YT_OPERATION_ID""""
        + s" $netOptBash"
        + s" ${bashCommand(ytsaurusJavaOptions: _*)}"
        + s" $driverOpts ${SparkAdapter.instance.defaultModuleOptions()}"
        + s" org.apache.spark.deploy.ytsaurus.DriverWrapper ${bashCommand(appArgs.mainClass)}"
        + s" ${bashCommand(additionalArgs: _*)} ${bashCommand(appArgs.driverArgs: _*)}"
      )

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

    conf.getAllWithPrefix("spark.ytsaurus.driverEnv.").foreach { v =>
      environment.put(v._1, YTree.stringNode(v._2))
    }

    val spec: Spec = (specBuilder, _, _) => {
      specBuilder.beginMap()
        .key("command").value(driverCommand)
        .key("job_count").value(1)
        .key("cpu_limit").value(conf.get(DRIVER_CORES))
        .key("memory_limit").value(memoryLimit)
      setCommonSpecParams(specBuilder, conf, DRIVER_TASK).endMap()
    }

    OperationParameters(spec, conf.get(YTSAURUS_MAX_DRIVER_FAILURES), "")
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

    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)

    val executorOpts = conf.get(EXECUTOR_JAVA_OPTIONS).getOrElse("")

    if (isPythonApp && conf.get(YTSAURUS_PYTHON_EXECUTABLE).isDefined) {
      environment.put("PYSPARK_EXECUTOR_PYTHON", YTree.stringNode(conf.get(YTSAURUS_PYTHON_EXECUTABLE).get))
    }

    if (conf.get(YTSAURUS_IS_PYTHON_BINARY)) {
      val binaryExecutable = Paths.get(conf.get(SPARK_PRIMARY_RESOURCE)).getFileName.toString
      environment.put("Y_BINARY_EXECUTABLE", YTree.stringNode(binaryExecutable))
    }

    conf.getExecutorEnv.foreach { v =>
      environment.put(v._1, YTree.stringNode(v._2))
    }

    val execCores = SparkAdapter.instance.getExecutorCores(execResources)

    var executorCommand = (
      s"$prepareEnvCommand && ${bashCommand(javaCommand, "-cp", sparkClassPath, s"-Xmx${execResources.executorMemoryMiB}m")}"
        + s" ${bashCommand(sparkJavaOpts: _*)} ${bashCommand(ytsaurusJavaOptions: _*)} $executorOpts"
        + s" org.apache.spark.executor.YTsaurusCoarseGrainedExecutorBackend"
        + s" --driver-url ${bashCommand(driverUrl)}"
        + """ --executor-id "$YT_TASK_JOB_INDEX""""
        + s" --cores ${bashCommand(execCores.toString)}"
        + s" --app-id ${bashCommand(appId)}"
        + """ --hostname "$HOSTNAME""""
      )

    executorCommand = addRedirectToStderrIfNeeded(conf, executorCommand)

    val memoryLimit = execResources.totalMemMiB * MIB
    val gpuLimit = execResources.customResources.get("gpu").map(_.amount).getOrElse(0L)

    val spec: Spec = (specBuilder, _, _) => {
      specBuilder.beginMap()

      setCommonSpecParams(specBuilder, conf, EXECUTOR_TASK)

      specBuilder.key("command").value(executorCommand)
        .key("job_count").value(numExecutors)
        .key("cpu_limit").value(execCores)
        .key("memory_limit").value(memoryLimit)

      if (gpuLimit > 0) {
        specBuilder
          .key("gpu_limit").value(gpuLimit)
          .key("cuda_toolkit_version").value(conf.get(YTSAURUS_CUDA_VERSION).get)
      }

      specBuilder.endMap()
    }

    val attemptId = s" [${sys.env.getOrElse("YT_TASK_JOB_INDEX", "0")}]"
    OperationParameters(spec, conf.get(YTSAURUS_MAX_EXECUTOR_FAILURES) * numExecutors, attemptId)
  }


  private def setCommonSpecParams(specBuilder: YTreeBuilder, conf: SparkConf, taskName: String): YTreeBuilder = {
    if (conf.contains(s"spark.ytsaurus.$taskName.task.parameters")) {
      val customParametersString = conf.get(s"spark.ytsaurus.$taskName.task.parameters")
      val customParameters = YTreeTextSerializer.deserialize(customParametersString).asMap().asScala
      customParameters.foreach { case (key, value) =>
        specBuilder.key(key).value(value)
      }
    }

    specBuilder
      .key("layer_paths").value(layerPaths)
      .key("file_paths").value(filePaths)
      .key("environment").value(environment)
      .key("enable_rpc_proxy_in_job_proxy").value(conf.get(YTSAURUS_RPC_JOB_PROXY_ENABLED))

    conf.get(YTSAURUS_NETWORK_PROJECT).foreach { networkProject =>
      specBuilder.key("network_project").value(networkProject)
    }

    specBuilder
  }
}

private[spark] object YTsaurusOperationManager extends Logging {

  def create(ytProxy: String, conf: SparkConf, networkName: Option[String], proxyRole: Option[String]): YTsaurusOperationManager = {
    logDebug("Entering YTsaurusOperationManager.create")
    val (user, token) = YTsaurusUtils.userAndToken(conf)
    logDebug("User and token were retrieved, now creating YTsaurus client")
    val ytClient: YTsaurusClient = buildClient(ytProxy, token, networkName, proxyRole, conf)
    logDebug("YTsaurus client has been successfully created")

    try {
      val globalConfigPath = conf.get(GLOBAL_CONFIG_PATH)
      logDebug("Retrieving global SPYT config")
      val globalConfig: YTreeMapNode = getDocument(ytClient, globalConfigPath)
      logDebug("Global SPYT config has been successfully retrieved")
      val isSquashFs = conf.get(YTSAURUS_SQUASHFS_ENABLED)

      if (!conf.contains(SPYT_VERSION)) {
        conf.set(SPYT_VERSION, BuildInfo.version)
      }
      val spytVersion = conf.get(SPYT_VERSION.key)
      logInfo(s"Used SPYT version: $spytVersion")
      val environment = globalConfig.getMap("environment")
      val javaHome = conf.get(YTSAURUS_JAVA_HOME)
      environment.put("JAVA_HOME", YTree.stringNode(javaHome))

      val releaseConfigPath = s"${conf.get(RELEASE_CONFIG_PATH)}/$spytVersion/${conf.get(LAUNCH_CONF_FILE)}"

      logDebug("Retrieving release SPYT config")
      val releaseConfig: YTreeMapNode = getDocument(ytClient, releaseConfigPath)
      logDebug("Release SPYT config has been successfully retrieved")

      val sv = org.apache.spark.SPARK_VERSION_SHORT
      val (svMajor, svMinor, svPatch) = VersionUtils.majorMinorPatchVersion(sv).get
      val distrRootPath = Seq(conf.get(SPARK_DISTRIBUTIVES_PATH), svMajor, svMinor, svPatch).mkString("/")
      val distrExtension = if (isSquashFs) ".squashfs" else ".tgz"
      logDebug("Looking for SPYT distributive path on cypress")
      val distrPathOpt = Some(distrRootPath).filter(path => ytClient.existsNode(path).join()).flatMap { path =>
        val distrRootContents = ytClient.listNode(path).join().asList()
        distrRootContents.asScala.find(_.stringValue().endsWith(distrExtension))
      }

      if (distrPathOpt.isEmpty) {
        throw new SparkException(s"Spark $sv ${distrExtension.substring(1)} distributive doesn't exist " +
          s"at path $distrRootPath on cluster $ytProxy")
      }

      val sparkDistr = distrPathOpt.get.stringValue()
      logDebug(s"SPYT distributive was found at ${sparkDistr}")

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

        if (isSquashFs) {
          val spytPackagePosOpt = (0 until filePathsList.size())
            .find(i => filePathsList.getString(i).endsWith("spyt-package.zip"))
          spytPackagePosOpt.foreach(pos => filePathsList.remove(pos))
        } else {
          filePathsList.add(YTree.stringNode(s"$distrRootPath/$sparkDistr"))
        }

        filePathsList
      }

      val layerPaths = getLayerPaths(conf, releaseConfig, s"$distrRootPath/$sparkDistr")

      enrichSparkConf(conf, releaseConfig)
      enrichSparkConf(conf, globalConfig)

      val javaCommand = s"$javaHome/bin/java"
      val home = "."
      val sparkHome = if (isSquashFs) "/usr/lib/spark" else s"$home/spark"
      val spytHome = if (isSquashFs) "/usr/lib/spyt" else s"$home/spyt-package"
      val sparkClassPath = s"$home/*:$spytHome/conf/:$spytHome/jars/*:$sparkHome/jars/*"
      environment.put("SPARK_HOME", YTree.stringNode(sparkHome))
      environment.put("SPYT_HOME", YTree.stringNode(spytHome))

      if (conf.get(Config.YTSAURUS_METRICS_ENABLED)) {
        enrichMetricsEnvironment(spytHome, conf, environment)
      }

      if (conf.get(YTSAURUS_IS_PYTHON_BINARY)) {
        val pyBinaryWrapper = YTree.stringNode(YTsaurusUtils.pythonBinaryWrapperPath(spytHome))
        environment.put("PYSPARK_EXECUTOR_PYTHON", pyBinaryWrapper)
        if (conf.get(SUBMIT_DEPLOY_MODE) == "cluster") {
          environment.put("PYSPARK_DRIVER_PYTHON", pyBinaryWrapper)
        }
      } else {
        environment.put("PYTHONPATH", YTree.stringNode(s"$spytHome/python"))
      }
      environment.put(
        "SPARK_YT_RPC_JOB_PROXY_ENABLED",
        YTree.stringNode(conf.get(YTSAURUS_RPC_JOB_PROXY_ENABLED).toString)
      )

      conf.set("spark.executor.resource.gpu.discoveryScript", s"$spytHome/bin/getGpusResources.sh")

      val ytsaurusJavaOptions = ArrayBuffer[String]()
      ytsaurusJavaOptions += s"$$(cat $spytHome/conf/java-opts)"
      if (conf.getBoolean("spark.hadoop.yt.preferenceIpv6.enabled", defaultValue = false)) {
        if (SparkVersionUtils.lessThan("3.4.0")) {
          ytsaurusJavaOptions += "-Djava.net.preferIPv6Addresses=true"
        } else {
          val driverJavaOptions = conf.get(DRIVER_JAVA_OPTIONS).getOrElse("")
          if (!driverJavaOptions.contains("java.net.preferIPv6Addresses")) {
            conf.set(DRIVER_JAVA_OPTIONS, s"-Djava.net.preferIPv6Addresses=true $driverJavaOptions")
          }
        }
      }

      if (!conf.contains(YTSAURUS_CUDA_VERSION)) {
        val cudaVersion = globalConfig.getStringO("cuda_toolkit_version").orElse("11.0")
        conf.set(YTSAURUS_CUDA_VERSION, cudaVersion)
      }

      val prepareEnvParameters = if (isSquashFs) {
        "--use-squashfs"
      } else {
        s"--spark-home $home --spark-distributive $sparkDistr"
      }

      val prepareEnvCommand = s"./setup-spyt-env.sh $prepareEnvParameters " +
        "&& export SPARK_LOCAL_DIRS=\"${YT_SPARK_LOCAL_DIRS:-/tmp}/${YT_OPERATION_ID}\"" // TODO: make a pretty filling

      logDebug("Creating YTsaurusOperationManager instance")
      new YTsaurusOperationManager(
        ytClient,
        user,
        token,
        layerPaths,
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

  private[ytsaurus] def getLayerPaths(conf: SparkConf,
                                      releaseConfig: YTreeMapNode,
                                      sparkDistr: String): YTreeNode = {
    val layerPaths = YTree.listBuilder().buildList()

    conf.get(YTSAURUS_EXTRA_PORTO_LAYER_PATHS).foreach(extraLayers => {
      extraLayers.split(',').foreach(layer => {
        layerPaths.add(YTree.stringNode(layer))
      })
    })

    var layerPathsKey = "layer_paths"

    if (conf.get(YTSAURUS_SQUASHFS_ENABLED)) {
      layerPaths.add(YTree.stringNode(s"${releaseConfig.getString("spark_yt_base_path")}/spyt-package.squashfs"))
      layerPaths.add(YTree.stringNode(sparkDistr))
      layerPathsKey = "squashfs_layer_paths"
    }

    val defaultLayers: YTreeListNode = releaseConfig.getListO(layerPathsKey).orElse(YTree.listBuilder().buildList())

    if (conf.contains(YTSAURUS_PORTO_LAYER_PATHS)) {
      conf.get(YTSAURUS_PORTO_LAYER_PATHS).foreach(layers => {
        layers.split(',').foreach(layer => {
          layerPaths.add(YTree.stringNode(layer))
        })
      })
    } else {
      defaultLayers.forEach(layer => {
        layerPaths.add(YTree.stringNode(layer.stringValue()))
      })
    }

    layerPaths
  }

  private[ytsaurus] def getDocument(ytClient: YTsaurusClient, path: String): YTreeMapNode = {
    if (ytClient.existsNode(path).join()) {
      ytClient.getNode(path).join().mapNode()
    } else {
      logWarning(s"Document at path $path does not exist")
      YTree.mapBuilder().buildMap()
    }
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
    logDebug(s"Uploading ${path} to cypress cache at ${remoteTempFilesDirectory}")
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

  private[ytsaurus] def enrichMetricsEnvironment(spytHome: String, conf: SparkConf, environment: YTreeMapNode): Unit = {
    logDebug(s"Initializing YT metrics environment")
    val sparkConfDir = Option(System.getenv("SPARK_CONF_DIR")).getOrElse(s"$spytHome/conf")
    val metricsConfPath = conf.get("spark.metrics.conf", s"$sparkConfDir/metrics.properties")
    logDebug(s"Loading metrics.properties from $metricsConfPath")
    val metricProps = loadPropertiesFromFile(metricsConfPath)

    val solomonPortKey = "*.sink.solomon.solomon_port"
    val metricsSparkPushPort = metricProps.getProperty(solomonPortKey)
    logDebug(s"Solomon port from properties: $metricsSparkPushPort")
    if (metricsSparkPushPort == null || metricsSparkPushPort.isEmpty) {
      throw new SparkException(s"Solomon port is not set in metrics.properties ($metricsConfPath)")
    }

    val metricsPullPort = conf.get(Config.YT_METRICS_PULL_PORT).toString
    val metricsAgentPullPort = conf.get(Config.YT_METRICS_AGENT_PULL_PORT).toString
    val appName = conf.get("spark.app.name")

    conf.set(s"$SPYT_ANNOTATIONS.is_spark", "True")
    conf.set(s"$SPYT_ANNOTATIONS.solomon_resolver_tag", "spark")
    conf.set(s"$SPYT_ANNOTATIONS.solomon_resolver_ports", s"$metricsPullPort,$metricsAgentPullPort")

    val envVars = Map(
      "YT_METRICS_SPARK_PUSH_PORT" -> metricsSparkPushPort,
      "YT_METRICS_PULL_PORT" -> metricsPullPort,
      "YT_AGENT_METRICS_PULL_PORT" -> metricsAgentPullPort,
      "YT_OPERATION_ALIAS" -> appName,
      "SPARK_YT_METRICS_ENABLED" -> "true"
    )

    envVars.foreach(kv => environment.put(kv._1, YTree.stringNode(kv._2)))

    logDebug(
      s"""YT metrics environment initialized with:
         |${envVars.map(kv => s"  ${kv._1}: ${kv._2}").mkString("\n")}""".stripMargin
    )
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
                          token: String,
                          networkName: Option[String],
                          proxyRole: Option[String],
                          conf: SparkConf
                         ): YTsaurusClient = {
    val builder: YTsaurusClient.ClientBuilder[_ <: YTsaurusClient, _] = YTsaurusClient.builder()
    builder.setCluster(ytProxy)
    conf.get(YTSAURUS_CLIENT_TIMEOUT).foreach { timeoutMilis =>
      val rpcOptions = new RpcOptions
      val timeoutDuration = Duration.ofMillis(timeoutMilis)
      rpcOptions.setGlobalTimeout(timeoutDuration)
      rpcOptions.setStreamingReadTimeout(timeoutDuration)
      rpcOptions.setStreamingWriteTimeout(timeoutDuration)
      builder.setConfig(YTsaurusClientConfig.builder().setRpcOptions(rpcOptions).build())
    }
    networkName.foreach(nn => builder.setProxyNetworkName(nn))
    proxyRole.foreach(pr => builder.setProxyRole(pr))
    if (token != null) {
      builder.setAuth(YTsaurusClientAuth.builder().setToken(token).build())
    }
    builder.build()
  }

  private[this] def loadPropertiesFromFile(path: String): Properties = {
    val properties = new Properties()
    try {
      YtUtils.tryUpdatePropertiesFromFile(path, properties)
    } catch {
      case e: Exception =>
        log.error(s"Error loading configuration file $path", e)
    }
    properties
  }
}

private[spark] case class YTsaurusOperation(id: GUID)

private[spark] case class OperationParameters(taskSpec: Spec, maxFailedJobCount: Int, attemptId: String)
