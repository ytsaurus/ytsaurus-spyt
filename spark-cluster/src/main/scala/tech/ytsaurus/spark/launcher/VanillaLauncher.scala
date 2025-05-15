package tech.ytsaurus.spark.launcher

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path, StandardCopyOption}
import scala.io.Source
import scala.util.{Failure, Success, Try}

trait VanillaLauncher {
  lazy val home: String = new File(sys.env.getOrElse("HOME", ".")).getAbsolutePath

  val sparkSystemProperties: Map[String, String] = tech.ytsaurus.spyt.wrapper.Utils.sparkSystemProperties

  val sparkHome: String = new File(env("SPARK_HOME", "./spark")).getAbsolutePath
  val spytHome: String = new File(env("SPYT_HOME", "./spyt-package")).getAbsolutePath

  def path(path: String): String = replaceHome(path)

  def env(name: String, default: => String): String = {
    replaceHome(sys.env.getOrElse(name, default))
  }

  def replaceHome(str: String): String = if (str == null) null else str.replaceAll("\\$HOME", home)

  def createFromTemplate(src: File)
                        (f: String => String): File = {
    val dst = new File(src.getAbsolutePath.replace(".template", ""))
    val is = Source.fromFile(src)
    val os = new FileWriter(dst)
    val res = Try(os.write(f(is.mkString)))
    is.close()
    os.close()
    res match {
      case Success(_) => dst
      case Failure(exception) => throw exception
    }
  }

  def profilingJavaOpt(port: Int) =
    s"-agentpath:/slot/sandbox/YourKit-JavaProfiler-2019.8/bin/linux-x86-64/libyjpagent.so=port=$port,listen=all"

  def isProfilingEnabled: Boolean = sys.env.get("SPARK_YT_PROFILING_ENABLED").exists(_.toBoolean)

  def prepareProfiler(): Unit = {
    import sys.process._
    import scala.language.postfixOps

    if (isProfilingEnabled) {
      val code = "unzip profiler.zip" !

      if (code != 0) {
        throw new IllegalStateException("Failed to unzip profiler")
      }
    }
  }

  def log4jConfigJavaOption(logJson: Boolean): String = {
    val log4jProperties = if (logJson) "log4j.clusterLogJson.properties" else "log4j.clusterLog.properties"
    val log4j2Properties = if (logJson) "log4j2.clusterLogJson.properties" else "log4j2.clusterLog.properties"
    s"-Dlog4j.configuration=file://$spytHome/conf/$log4jProperties -Dlog4j2.configurationFile=file://$spytHome/conf/$log4j2Properties"
  }
}
