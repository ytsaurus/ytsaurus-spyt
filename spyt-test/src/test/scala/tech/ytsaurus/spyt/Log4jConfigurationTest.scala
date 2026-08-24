package tech.ytsaurus.spyt

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.{Configuration, ConfigurationFactory, ConfigurationSource}
import org.apache.logging.log4j.status.{StatusData, StatusListener, StatusLogger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileInputStream}
import java.net.URLClassLoader
import scala.collection.mutable
import scala.io.Source

class Log4jConfigurationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  behavior of "SPYT log4j2 configurations"

  private val appenderNamePattern = "^appender\\.[^.]+\\.name\\s*=\\s*(\\S+)\\s*$".r
  private val confDir = new File("../spyt-package/src/main/spark-extra/conf")
  private val logDir = new File("logs")
  private val logDirExisted = logDir.exists()

  // must be in sync with VanillaLauncher.log4jConfigJavaOption
  private val clusterConfigNames = Seq("log4j2.clusterLog.properties", "log4j2.clusterLogJson.properties")

  private val configFiles: Seq[File] = confDir
    .listFiles((_, name) => name.startsWith("log4j2") && name.endsWith(".properties"))
    .sortBy(_.getName)
    .toSeq

  override def afterAll(): Unit = {
    if (!logDirExisted) deleteRecursively(logDir)
    super.afterAll()
  }

  configFiles.foreach { file =>
    it should s"be built by log4j2 with all declared appenders: ${file.getName}" in {
      val (configuration, errors) = configure(file)

      errors.map(_.getFormattedStatus) shouldBe empty
      declaredAppenders(file).filter(configuration.getAppenders.get(_) == null) shouldBe empty
    }
  }

  clusterConfigNames.foreach { name =>
    it should s"be shipped in spyt-package: $name" in {
      configFiles.map(_.getName) should contain(name)
    }
  }

  private def declaredAppenders(file: File): Seq[String] = {
    val source = Source.fromFile(file)
    try {
      source.getLines().flatMap(appenderNamePattern.findFirstMatchIn).map(_.group(1)).toList
    } finally {
      source.close()
    }
  }

  private def configure(file: File): (Configuration, Seq[StatusData]) = {
    val errors = mutable.ListBuffer.empty[StatusData]
    val listener = new StatusListener {
      override def log(data: StatusData): Unit = errors += data
      override def getStatusLevel: Level = Level.ERROR
      override def close(): Unit = ()
    }
    StatusLogger.getLogger.registerListener(listener)
    try {
      val configuration = withConfDirInClassLoader {
        val input = new FileInputStream(file)
        try {
          val source = new ConfigurationSource(input, file)
          val configuration = ConfigurationFactory.getInstance.getConfiguration(null, source)
          configuration.initialize()
          configuration
        } finally {
          input.close()
        }
      }
      (configuration, errors.toList)
    } finally {
      StatusLogger.getLogger.removeListener(listener)
    }
  }

  private def withConfDirInClassLoader[T](body: => T): T = {
    val thread = Thread.currentThread()
    val classLoader = thread.getContextClassLoader
    thread.setContextClassLoader(new URLClassLoader(Array(confDir.toURI.toURL), classLoader))
    try {
      body
    } finally {
      thread.setContextClassLoader(classLoader)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
  }
}
