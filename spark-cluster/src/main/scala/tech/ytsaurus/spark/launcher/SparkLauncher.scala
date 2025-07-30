package tech.ytsaurus.spark.launcher

import io.circe.generic.auto._
import io.circe.parser._
import org.apache.spark.deploy.history.YtHistoryServer
import org.apache.spark.deploy.master.YtMaster
import org.apache.spark.deploy.worker.YtWorker
import org.slf4j.{Logger, LoggerFactory}
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spark.launcher.Service.{BasicService, MasterService}
import tech.ytsaurus.spyt.wrapper.Utils.{bashCommand, parseDuration, ytHostnameOrIpAddress}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider}
import tech.ytsaurus.spyt.wrapper.discovery._
import tech.ytsaurus.spyt.{HostAndPort, SparkAdapter, SparkVersionUtils}

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}

trait SparkLauncher {
  self: VanillaLauncher =>

  private val log = LoggerFactory.getLogger(getClass)
  private val masterClass = classOf[YtMaster].getName
  private val workerClass = classOf[YtWorker].getName
  private val historyServerClass = classOf[YtHistoryServer].getName
  private val commonJavaOpts = configureJavaOptions()

  case class SparkDaemonConfig(memory: String,
                               startTimeout: Duration)

  object SparkDaemonConfig {
    def fromProperties(daemonName: String,
                       defaultMemory: String): SparkDaemonConfig = {
      SparkDaemonConfig(
        sparkSystemProperties.getOrElse(s"spark.$daemonName.memory", defaultMemory),
        sparkSystemProperties.get(s"spark.$daemonName.timeout").map(parseDuration).getOrElse(5 minutes)
      )
    }
  }

  def absolutePath(string: String) = if (string == null) null else new File(string).getAbsolutePath

  private val livyHome: String = absolutePath(env("LIVY_HOME", "./livy"))
  private val livyWorkDir: String = absolutePath(env("LIVY_WORK_DIR", "./livy"))

  private val javaHome: String = absolutePath(env("JAVA_HOME", null))

  val clusterVersion: String = sys.env("SPYT_CLUSTER_VERSION")
  val processCheckRetries: Int = sys.env.getOrElse("SPYT_CLUSTER_PROCESS_CHECK_RETRIES", "5").toInt

  def createLivyWorkDir(): Unit = {
    Files.createDirectories(Path.of(livyWorkDir, "conf"))
  }

  def prepareLivyLog4jConfig(): Unit = {
    val src = Path.of(spytHome, "conf", "log4j.livy.properties")
    val dst = Path.of(livyWorkDir, "conf", "log4j.properties")
    Files.copy(src, dst)
  }

  def prepareLivyConf(hostAndPort: HostAndPort, masterAddress: String, maxSessions: Int): Unit = {
    val src = Path.of(home, "livy.template.conf")
    val preparedConfPath = createFromTemplate(src.toFile) { content =>
      content
        .replaceAll("\\$BIND_PORT", hostAndPort.port.toString)
        .replaceAll("\\$MASTER_ADDRESS", masterAddress)
        .replaceAll("\\$MAX_SESSIONS", maxSessions.toString)
    }.toPath
    val dst = Path.of(livyWorkDir, "conf", "livy.conf")

    Files.copy(preparedConfPath, dst)
  }

  private val isIpv6PreferenceEnabled: Boolean = {
    sparkSystemProperties.get("spark.hadoop.yt.preferenceIpv6.enabled").exists(_.toBoolean)
  }

  private def getLivyClientSparkConf(networkProject: Option[String], enableSquashfs: Boolean): Seq[String] = {
    val ipv6Conf = if (isIpv6PreferenceEnabled) {
      Seq("spark.driver.extraJavaOptions = -Djava.net.preferIPv6Addresses=true") ++ (
        if (SparkVersionUtils.lessThan("3.4.0")) {
          // Starting from spark 3.4.0 IPv6 preference for executors is dependent on driver
          Seq("spark.executor.extraJavaOptions = -Djava.net.preferIPv6Addresses=true")
        } else {
          Nil
        })
    } else {
      Nil
    }

    val networkProjectConf = networkProject.toSeq.map { np =>
      s"spark.ytsaurus.network.project = $np"
    }

    val squashfsConf = Seq("spark.ytsaurus.squashfs.enabled = true").filter(_ => enableSquashfs)

    ipv6Conf ++ networkProjectConf ++ squashfsConf
  }

  def prepareLivyClientConf(driverCores: Int,
                            driverMemory: String,
                            networkProject: Option[String],
                            enableSquashfs: Boolean): Unit = {
    val src = Path.of(home, "livy-client.template.conf")
    val preparedConfPath = createFromTemplate(src.toFile) { content =>
      content
        .replaceAll("\\$LIVY_ADDRESS", ytHostnameOrIpAddress)
        .replaceAll("\\$DRIVER_CORES", driverCores.toString)
        .replaceAll("\\$DRIVER_MEMORY", driverMemory)
        .replaceAll("\\$EXTRA_SPARK_CONF", getLivyClientSparkConf(networkProject, enableSquashfs).mkString("\n"))
    }.toPath
    val dst = Path.of(livyWorkDir, "conf", "livy-client.conf")

    Files.copy(preparedConfPath, dst)
  }

  private def configureJavaOptions(): Seq[String] = {
    if (isIpv6PreferenceEnabled) {
      Seq("-Djava.net.preferIPv6Addresses=true")
    } else {
      Seq()
    }
  }

  // https://archive.apache.org/dist/spark/docs/3.2.2/spark-standalone.html#starting-a-cluster-manually
  private def injectYTEnvPortsToArgs(positionalArgs: ArrayBuffer[String]): Unit = {
    for (port <- sys.env.get("YT_PORT_0")) {
      positionalArgs += "--port"
      positionalArgs += port
    }
    for (port <- sys.env.get("YT_PORT_1")) {
      positionalArgs += "--webui-port"
      positionalArgs += port
    }
  }

  def startMaster(reverseProxyUrl: Option[String]): MasterService = {
    log.info("Start Spark master")
    val config = SparkDaemonConfig.fromProperties("master", "512M")
    reverseProxyUrl.foreach(url => log.info(f"Reverse proxy url is $url"))
    val reverseProxyUrlProp = reverseProxyUrl.map(url => f"-Dspark.ui.reverseProxyUrl=$url")
    val positionalArgs = ArrayBuffer[String]()
    injectYTEnvPortsToArgs(positionalArgs)
    val thread = runSparkThread(
      masterClass,
      config.memory,
      positionalArgs = positionalArgs,
      namedArgs = Map("host" -> ytHostnameOrIpAddress),
      systemProperties = commonJavaOpts ++ reverseProxyUrlProp.toSeq
    )
    val address = readAddressOrDie("master", config.startTimeout, thread)
    MasterService("Master", address, thread)
  }

  case class BasicServiceWithUi(name: String, address: HostAndPort, webUiUrl: URI, thread: Thread) extends ServiceWithAddress

  def startWorker(master: Address,
                  cores: Int,
                  memory: String,
                  extraEnv: Map[String, String],
                  enableSquashfs: Boolean): BasicServiceWithUi = {
    val config = SparkDaemonConfig.fromProperties("worker", "512M")
    val positionalArgs = ArrayBuffer[String]()
    injectYTEnvPortsToArgs(positionalArgs)
    positionalArgs += s"spark://${master.hostAndPort}"
    val thread = runSparkThread(
      workerClass,
      config.memory,
      namedArgs = Map(
        "cores" -> cores.toString,
        "memory" -> memory,
        "host" -> ytHostnameOrIpAddress
      ) ++ (if (enableSquashfs) Map("work-dir" -> s"$home/work") else Map()),
      positionalArgs = positionalArgs,
      systemProperties = commonJavaOpts,
      extraEnv = extraEnv
    )
    val address = readAddressOrDie("worker", config.startTimeout, thread)

    BasicServiceWithUi("Worker", address.hostAndPort, address.webUiUri, thread)
  }

  def startHistoryServer(path: String, memory: String, discoveryService: DiscoveryService): BasicService = {
    val javaOpts = Seq(
      discoveryService.masterWrapperEndpoint().map(hp => s"-Dspark.hadoop.yt.masterWrapper.url=$hp"),
      Some(profilingJavaOpt(27111)).filter(_ => isProfilingEnabled),
      Some(s"-Dspark.history.fs.logDirectory=$path")
    )
    val config = SparkDaemonConfig.fromProperties("history", memory)

    val thread = runSparkThread(
      historyServerClass,
      config.memory,
      systemProperties = javaOpts.flatten ++ commonJavaOpts
    )
    val address = readAddressOrDie("history", config.startTimeout, thread)
    BasicService("Spark History Server", address.hostAndPort, thread)
  }

  private def readLivyLogs(): String = {
    Seq("bash", "-c", """cat "$0"/logs/livy-*-server.out""", livyWorkDir).!!
  }

  private def runLivyProcess(log: Logger): Process = {
    val livyJavaOpts = s"cat $spytHome/conf/java-opts".!!.trim + " " + SparkAdapter.instance.defaultModuleOptions()
    val livyRunner = f"$livyHome/bin/livy-server start"
    log.info(s"Run command: $livyRunner")
    val extraEnv = ArrayBuffer(
      "SPARK_HOME" -> sparkHome,
      "SPARK_CONF_DIR" -> s"$spytHome/conf",
      "LIVY_PID_DIR" -> livyWorkDir,
      "LIVY_CONF_DIR" -> s"$livyWorkDir/conf",
      "LIVY_LOG_DIR" -> s"$livyWorkDir/logs",
      "PYSPARK_PYTHON" -> "python3",
      "LIVY_SERVER_JAVA_OPTS" -> livyJavaOpts,
      "CLASSPATH" -> s"$spytHome/jars/*:$sparkHome/jars/*",
      "PYTHONPATH" -> s"$spytHome/python",
    )
    if (javaHome != null) {
      extraEnv += ("JAVA_HOME" -> javaHome)
    }
    val startProcess = Process(livyRunner, new File("."), extraEnv: _*).run(ProcessLogger(log.info(_)))
    val startProcessCode = startProcess.exitValue()
    log.info(f"Server started. Code: $startProcessCode")
    if (startProcessCode == 0) {
      val pid = Seq("bash", "-c", """cat "$0"/livy-*-server.pid""", livyWorkDir).!!.trim
      log.info(f"Attaching to livy process with pid $pid...")
      Process("bash", Seq("-c", """tail -f -n 0 "$0"/logs/livy-*-server.out""", livyWorkDir)).run(ProcessLogger(log.info(_)))
      Process("bash", Seq("-c", """tail --pid="$0" -f /dev/null""", pid)).run(ProcessLogger(log.info(_)))
    } else {
      startProcess
    }
  }

  def startLivyServer(address: HostAndPort): BasicService = {
    val thread = runDaemonThread(() => {
      var process: Process = null
      try {
        val log = LoggerFactory.getLogger(self.getClass)
        process = runLivyProcess(log)
        log.warn(s"Livy exit value: ${process.exitValue()}")
      } catch {
        case e: Throwable =>
          log.error(s"Livy failed with error: ${e.getMessage}")
          if (process != null) {
            process.destroy()
            log.info("Livy process destroyed")
          }
      }
      log.info("Logs:\n" + readLivyLogs())
    }, "Livy thread")
    BasicService("Livy Server", address, thread)
  }

  private def readAddressOrDie(name: String, timeout: Duration, thread: Thread): Address = {
    Try(readAddress(name, timeout, thread)) match {
      case Success(address) => address
      case Failure(exception) =>
        thread.interrupt()
        throw exception
    }
  }

  private def readAddress(name: String, timeout: Duration, thread: Thread): Address = {
    val successFlag = new File(s"${name}_address_success")
    val file = new File(s"${name}_address")
    if (!DiscoveryService.waitFor(successFlag.exists() || !thread.isAlive, timeout, s"$name address in file $file")) {
      throw new RuntimeException(s"The process of $name is stucked")
    }
    if (!thread.isAlive) {
      throw new RuntimeException(s"The process of $name was failed")
    }
    val source = Source.fromFile(file)
    try {
      decode[Address](source.mkString) match {
        case Right(address) => address
        case Left(error) => throw error
      }
    } finally {
      source.close()
    }
  }

  private def runSparkThread(className: String,
                             memory: String,
                             systemProperties: Seq[String] = Nil,
                             namedArgs: Map[String, String] = Map.empty,
                             positionalArgs: Seq[String] = Nil,
                             extraEnv: Map[String, String] = Map.empty): Thread = {
    runDaemonThread(() => {
      var process: Process = null
      try {
        val log = LoggerFactory.getLogger(self.getClass)
        process = runSparkClass(className, systemProperties, namedArgs, positionalArgs, memory, log, extraEnv)
        log.warn(s"Spark exit value: ${process.exitValue()}")
      } catch {
        case e: Throwable =>
          log.error(s"Spark failed with error: ${e.getMessage}")
          process.destroy()
          log.info("Spark process destroyed")
      }
    }, "Spark Thread")
  }

  private def runDaemonThread(runnable: Runnable, threadName: String = ""): Thread = {
    val thread = new Thread(runnable, threadName)
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def runSparkClass(className: String,
                            systemProperties: Seq[String],
                            namedArgs: Map[String, String],
                            positionalArgs: Seq[String],
                            memory: String,
                            log: Logger,
                            extraEnv: Map[String, String]): Process = {
    val command = Seq(s"$sparkHome/bin/spark-class", className) ++
      namedArgs.flatMap { case (k, v) => Seq(s"--$k", v) } ++ positionalArgs

    log.info(s"Run command: $command")

    val workerLog4j = s"-Dlog4j.configuration=file://$spytHome/conf/log4j.worker.properties"
    val sparkLocalDirs = env("SPARK_LOCAL_DIRS", "./tmpfs")
    val processExtraEnv = ArrayBuffer(
      "SPARK_HOME" -> sparkHome,
      "SPARK_CONF_DIR" -> s"$spytHome/conf",
      "PYTHONPATH" -> s"$spytHome/python",
      "SPARK_LOCAL_DIRS" -> sparkLocalDirs,
      // when using MTN, Spark should use ip address and not hostname, because hostname is not in DNS
      "SPARK_LOCAL_HOSTNAME" -> ytHostnameOrIpAddress,
      "SPARK_DAEMON_MEMORY" -> memory,
      "SPARK_DAEMON_JAVA_OPTS" -> bashCommand(
        Seq(workerLog4j)
          ++ systemProperties
          ++ sparkSystemProperties.map { case (k, v) => s"-D$k=$v" }: _*
        ),
    )
    if (javaHome != null) {
      processExtraEnv += ("JAVA_HOME" -> javaHome)
    }
    processExtraEnv ++= extraEnv.toList
    Process(command, new File("."), processExtraEnv: _*).run(ProcessLogger(log.info(_)))
  }

  @tailrec
  final def checkPeriodically(p: => Boolean): Unit = {
    if (p) {
      Thread.sleep((20 seconds).toMillis)
      checkPeriodically(p)
    }
  }

  def withYtClient(ytConfig: YtClientConfiguration)
                  (f: CompoundClient => Unit): Unit = {
    f(YtClientProvider.ytClient(ytConfig))
  }

  def withOptionalYtClient(ytConfig: Option[YtClientConfiguration])
                          (f: Option[CompoundClient] => Unit): Unit = {
    if (ytConfig.isDefined) {
      withYtClient(ytConfig.get) { yt => f(Some(yt)) }
    } else {
      f(None)
    }
  }

  def withCypressDiscovery(baseDiscoveryPath: String, client: CompoundClient)
                          (f: DiscoveryService => Unit): Unit = {
    val service = new CypressDiscoveryService(baseDiscoveryPath)(client)
    f(service)
  }

  def withCypressDiscovery(baseDiscoveryPath: Option[String], client: Option[CompoundClient])
                          (f: Option[DiscoveryService] => Unit): Unit = {
    if (baseDiscoveryPath.isDefined && client.isDefined) {
      withCypressDiscovery(baseDiscoveryPath.get, client.get)(x => f(Some(x)))
    } else {
      f(None)
    }
  }

  def withDiscoveryServer(groupId: Option[String], masterGroupId: Option[String])
                         (f: Option[DiscoveryService] => Unit): Unit = {
    if (sys.env.contains("YT_DISCOVERY_ADDRESSES") && groupId.isDefined) {
      val client = YtWrapper.createDiscoveryClient()
      try {
        val service = new DiscoveryServerService(client, groupId.get, masterGroupId)
        f(Some(service))
      } finally {
        log.info("Close discovery client")
        client.close()
      }
    } else {
      f(None)
    }
  }

  def withCompoundDiscovery(groupId: Option[String], masterGroupId: Option[String],
                            baseDiscoveryPath: Option[String], client: Option[CompoundClient])
                           (f: DiscoveryService => Unit): Unit = {
    withCypressDiscovery(baseDiscoveryPath, client) { cypressDiscovery =>
      withDiscoveryServer(groupId, masterGroupId) { serverDiscovery =>
        val compoundService = new CompoundDiscoveryService(Seq(cypressDiscovery, serverDiscovery).flatten)
        f(compoundService)
      }
    }
  }

  def getFullGroupId(groupId: String): String = "/spyt/" + groupId

  def withService[T, S <: Service](service: S)(f: S => T): T = {
    try f(service) finally service.stop()
  }

  def withOptionalService[T, S <: Service](service: Option[S])(f: Option[S] => T): T = {
    try f(service) finally service.foreach(_.stop())
  }

  def waitForMaster(timeout: Duration, ds: DiscoveryService): Address = {
    log.info("Waiting for master http address")
    ds.waitAddress(timeout)
      .getOrElse(throw new IllegalStateException(s"Empty discovery path or master is not running for $timeout"))
  }
}
