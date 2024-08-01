
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.deploy.ytsaurus.Config._
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.ytsaurus.{ApplicationArguments, Config, YTsaurusUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{Utils, VersionUtils}
import tech.ytsaurus.client.YTsaurusClient
import tech.ytsaurus.client.operations.{Spec, VanillaSpec}
import tech.ytsaurus.client.request.{CompleteOperation, GetOperation, UpdateOperationParameters, VanillaOperation}
import tech.ytsaurus.client.rpc.YTsaurusClientAuth
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree.{YTree, YTreeListNode, YTreeMapNode, YTreeNode, YTreeTextSerializer}

import java.nio.file.Paths
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[spark] class YTsaurusOperationManager(
    val ytClient: YTsaurusClient,
    user: String,
    token: String,
    portoLayers: YTreeNode,
    filePaths: YTreeNode,
    pythonPaths: YTreeMapNode,
  environment: YTreeMapNode,
    home: String,
    prepareEnvCommand: String,
    sparkClassPath: String,
    javaCommand: String,
    ytsaurusJavaOptions: Seq[String],
    unpackArchivesCommand: String)
  extends Logging {
  import YTsaurusOperationManager._

  def startDriver(conf: SparkConf, appArgs: ApplicationArguments): YTsaurusOperation = {
    val opParams = driverParams(conf, appArgs)
    val operation = startVanillaOperation(conf, DRIVER_TASK, opParams)
    conf.set(Config.DRIVER_OPERATION_ID, operation.id.toString)
    logInfo(s"Driver operation ID: ${operation.id}")
    operation
  }

  def startExecutors(
    sc: SparkContext,
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
      if (!isFinalState(getOperationState(getOperation(operation)))) {
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

  def getOperation(operation: YTsaurusOperation): YTreeNode = {
    val request = GetOperation.builder().setOperationId(operation.id).build()
    ytClient.getOperation(request).join()
  }

  private def createSpec(conf: SparkConf, taskName: String, opParams: OperationParameters): VanillaSpec = {
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

    val additionalParameters: Map[String, YTreeNode] = Map(
      "secure_vault" -> secureVault,
      "max_failed_job_count" -> YTree.integerNode(opParams.maxFailedJobCount),
      "preemption_mode" -> YTree.stringNode("normal"),
      "title" -> YTree.stringNode(title),
    ) ++ poolParameters ++ userParameters

    opSpecBuilder.setAdditionalSpecParameters(additionalParameters.asJava)

    opSpecBuilder.build()
  }

  private def driverParams(conf: SparkConf, appArgs: ApplicationArguments): OperationParameters = {
    val driverMemoryMiB = conf.get(DRIVER_MEMORY)

    val sparkJavaOpts = Utils.sparkJavaOpts(conf).map { opt =>
      val Array(k, v) = opt.split("=", 2)
      k + "=\"" + v + "\""
    }

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
      unpackArchivesCommand,
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

  private def executorParams(
    conf: SparkConf,
    appId: String,
    resourceProfile: ResourceProfile,
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

    val execEnvironmentBuilder = environment.toMapBuilder

    if (isPythonApp && conf.get(YTSAURUS_PYTHON_VERSION).isDefined) {
      val pythonVersion = conf.get(YTSAURUS_PYTHON_VERSION).get
      val pythonPathOpt = pythonPaths.get(pythonVersion)
      if (pythonPathOpt.isEmpty) {
        throw new SparkException(
          s"Python version $pythonVersion is not supported. Please check the path specified in " +
            s"$GLOBAL_CONFIG_PATH for supported versions"
        )
      }
      execEnvironmentBuilder.key("PYSPARK_EXECUTOR_PYTHON").value(pythonPathOpt.get())
    }

    var executorCommand = (Seq(
      unpackArchivesCommand,
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
        .key("environment").value(execEnvironmentBuilder.endMap().build())
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

  def create(ytProxy: String, conf: SparkConf, networkName: Option[String]): YTsaurusOperationManager = {
    val (user, token) = YTsaurusUtils.userAndToken(conf)
    val ytClient: YTsaurusClient = buildClient(ytProxy, user, token, networkName)

    try {
      val globalConfigPath = conf.get(GLOBAL_CONFIG_PATH)
      val globalConfig: YTreeMapNode = getDocument(ytClient, globalConfigPath)

      val spytVersion = conf.get(SPYT_VERSION).getOrElse(getLatestRelease(ytClient, conf))
      logInfo(s"Used SPYT version: $spytVersion")
      val environment = globalConfig.getMap("environment")
      val javaHome = environment.getStringO("JAVA_HOME")
        .orElseThrow(() => new SparkException("JAVA_HOME is not set in " +
          s"${GLOBAL_CONFIG_PATH.key} parameter value"))

      val releaseConfigPath = s"${conf.get(RELEASE_CONFIG_PATH)}/$spytVersion/${conf.get(LAUNCH_CONF_FILE)}"

      val releaseConfig: YTreeMapNode = getDocument(ytClient, releaseConfigPath)

      val portoLayers = getPortoLayers(conf, releaseConfig.getListO("layer_paths").orElse(YTree.listBuilder().buildList()))

      val filePaths = releaseConfig.getListO("file_paths").orElse(YTree.listBuilder().buildList())
      val pythonPaths = globalConfig.getMapO("python_cluster_paths").orElse(YTree.mapBuilder().buildMap())
      val cudaVersion = globalConfig.getStringO("cuda_toolkit_version").orElse("11.0")

      val unpackArchivesCommands = applicationFiles(conf).map { file =>
        filePaths.add(file.toNode())
        file.unpackCommand
      }.filter(cmd => cmd.isDefined).map(cmd => cmd.get)

      val unpackArchivesCommand = if (unpackArchivesCommands.nonEmpty) {
        unpackArchivesCommands.mkString(" && ") + " &&"
      } else {
        ""
      }


      val sv = org.apache.spark.SPARK_VERSION_SHORT
      val (svMajor, svMinor, svPatch) = VersionUtils.majorMinorPatchVersion(sv).get
      val distrRootPath = Seq(conf.get(SPARK_DISTRIBUTIVES_PATH), svMajor, svMinor, svPatch).mkString("/")
      val distrTgzOpt = Some(distrRootPath).filter(path => ytClient.existsNode(path).join()).flatMap { path =>
        val distrRootContents = ytClient.listNode(path).join().asList()
        distrRootContents.asScala.find(_.stringValue().endsWith(".tgz"))
      }

      distrTgzOpt match {
        case Some(sparkTgz) => filePaths.add(YTree.stringNode(s"$distrRootPath/${sparkTgz.stringValue()}"))
        case _ => throw new SparkException(s"Spark $sv tgz distributive doesn't exist " +
          s"at path $distrRootPath on cluster $ytProxy")
      }

      val sparkTgz = distrTgzOpt.get.stringValue()

      enrichSparkConf(conf, releaseConfig)
      enrichSparkConf(conf, globalConfig)

      val javaCommand = s"$javaHome/bin/java"
      val home = "."
      val sparkHome = s"$home/spark"
      val spytHome = s"$home/spyt-package"
      val sparkClassPath = s"$home/*:$spytHome/conf/:$spytHome/jars/*:$sparkHome/jars/*"
      environment.put("SPARK_HOME", YTree.stringNode(sparkHome))
      environment.put("PYTHONPATH", YTree.stringNode(s"$spytHome/python"))

      conf.set("spark.executor.resource.gpu.discoveryScript", s"$spytHome/bin/getGpusResources.sh")

      val ytsaurusJavaOptions = ArrayBuffer[String]()
      if (conf.getBoolean("spark.hadoop.yt.preferenceIpv6.enabled", false)) {
        ytsaurusJavaOptions += "-Djava.net.preferIPv6Addresses=true"
      }
      ytsaurusJavaOptions += s"$$(cat $spytHome/conf/java-opts)"

      if (!conf.contains(YTSAURUS_CUDA_VERSION)) {
        conf.set(YTSAURUS_CUDA_VERSION, cudaVersion)
      }

      val prepareEnvCommand = s"./setup-spyt-env.sh --spark-home $home --spark-distributive $sparkTgz"

      new YTsaurusOperationManager(
        ytClient,
        user,
        token,
        portoLayers,
        filePaths,
        pythonPaths,
        environment,
        home,
        prepareEnvCommand,
        sparkClassPath,
        javaCommand,
        ytsaurusJavaOptions,
        unpackArchivesCommand
      )
    } catch {
      case t: Throwable =>
        ytClient.close()
        throw t
    }
  }

  private[ytsaurus] def getPortoLayers(conf: SparkConf, defaultLayers: YTreeListNode): YTreeNode = {
    val layers = conf.get(YTSAURUS_PORTO_LAYER_PATHS).map(layers => {
      val builder = YTree.listBuilder()
      layers.split(',').foreach(layer => {
        builder.value(YTree.stringNode(layer))
      })
      builder.buildList()
    }).getOrElse(defaultLayers)
    conf.get(YTSAURUS_EXTRA_PORTO_LAYER_PATHS).foreach(extraPortoLayers => {
      extraPortoLayers.split(',').foreach(layer => {
        layers.add(YTree.stringNode(layer))
      })
    })
    layers
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

  private[ytsaurus] case class ApplicationFile(val ytPath: String, val alias: Option[String], val unpackCommand: Option[String]) {
    def toNode(): YTreeNode = {
      val node = YTree.stringNode(ytPath)
      alias.foreach(alias => {
        node.putAttribute("file_name", YTree.stringNode(alias))
      })
      node
    }
  }

  private[ytsaurus] def applicationFiles(conf: SparkConf): Seq[ApplicationFile] = {
    val providedLists = conf.get(JARS) ++
      conf.get(FILES) ++
      conf.get(SUBMIT_PYTHON_FILES)

    val primaryResource = conf.get(SPARK_PRIMARY_RESOURCE)

    val primaryResourceSeq =
      if (primaryResource != null && primaryResource != SparkLauncher.NO_RESOURCE) {
        Seq(primaryResource)
      } else {
        Seq.empty
      }

    val archives = conf.get(ARCHIVES).filter(_.startsWith("yt:/")).map(YtWrapper.formatPath).distinct.zipWithIndex.map { case (fullPath, archiveIndex) =>
      val (ytPath, directoryName) = if (fullPath.contains('#')) {
        val parts = fullPath.split('#')
        if (parts.length != 2) {
          throw new SparkException(s"Too many '#': $fullPath")
        }
        (parts(0), parts(1))
      } else {
        (fullPath, fullPath.split('/').last)
      }
      // to avoid collisions
      val archiveFileName = s"__dep-$archiveIndex-" + ytPath.split('/').last

      val unpackCommand = if (archiveFileName.endsWith(".zip")) {
        s"unzip $archiveFileName -d $directoryName"
      } else if (archiveFileName.endsWith(".tar.gz") || archiveFileName.endsWith(".tgz")) {
        s"mkdir $directoryName && tar -xzvf $archiveFileName -C $directoryName"
      } else if (archiveFileName.endsWith(".tar.bz2")) {
        s"mkdir $directoryName && tar -xjvf $archiveFileName -C $directoryName"
      } else if (archiveFileName.endsWith(".tar")) {
        s"mkdir $directoryName && tar -xvf $archiveFileName -C $directoryName"
      } else {
        throw new SparkException(s"Archive type is not supported: $fullPath")
      }

      new ApplicationFile(ytPath, Some(archiveFileName), Some(unpackCommand))
    }

    (providedLists ++ primaryResourceSeq).filter(_.startsWith("yt:/")).map(YtWrapper.formatPath).distinct.map(x => new ApplicationFile(x, None, None)) ++ archives
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
          val confValue = conf.getBoolean(enabler, false)
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

  def isFinalState(currentState: String): Boolean = {
    finalStates.contains(currentState)
  }

  def getWebUIAddress(operation: YTreeNode): Option[String] = {
    val description = WEB_UI_PATH.foldLeft(Some(operation).asInstanceOf[Option[YTreeNode]]) { (yTreeOpt, key) =>
      yTreeOpt.map(_.asMap()).filter(_.containsKey(key)).map(_.get(key))
    }

    description.filter(_.isStringNode).map(_.stringNode().getValue)
  }

  private def buildClient(ytProxy: String, user: String, token: String, networkName: Option[String]): YTsaurusClient = {
    val builder: YTsaurusClient.ClientBuilder[_ <: YTsaurusClient, _] = YTsaurusClient.builder()
    builder.setCluster(ytProxy)
    if (networkName.isDefined) {
      builder.setProxyNetworkName(networkName.get)
    }
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
