package tech.ytsaurus.spyt

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.{Configuration, ConfigurationFactory, ConfigurationSource}
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.core.{LoggerContext, StringLayout}
import org.apache.logging.log4j.message.SimpleMessage
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.wrapper.model.{WorkerLogBlock, WorkerLogBlockInner}

import java.io.{File, FileInputStream}
import java.net.URLClassLoader
import java.time.{Instant, LocalDateTime, ZoneOffset}

class WorkerLogJsonLayoutTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  behavior of "console layout of log4j2.clusterLogJson.properties"

  private val confDir = new File("../spyt-package/src/main/spark-extra/conf")
  private val configFile = new File(confDir, "log4j2.clusterLogJson.properties")
  private val logDir = new File("logs")
  private val logDirExisted = logDir.exists()

  private val eventMillis = 1755000000123L
  private val fileCreationTime = LocalDateTime.of(2000, 1, 1, 0, 0)

  private lazy val configuration: Configuration = withConfDirInClassLoader {
    val input = new FileInputStream(configFile)
    try {
      val source = new ConfigurationSource(input, configFile)
      val configuration = ConfigurationFactory.getInstance.getConfiguration(LoggerContext.getContext(false), source)
      configuration.initialize()
      configuration.start()
      configuration
    } finally {
      input.close()
    }
  }

  private lazy val parsedEvent: WorkerLogBlockInner =
    WorkerLogBlockInner.fromJson(formatEvent(), fileCreationTime)

  override def afterAll(): Unit = {
    configuration.stop()
    if (!logDirExisted) deleteRecursively(logDir)
    super.afterAll()
  }

  it should "write events that worker log service parses into columns" in {
    parsedEvent.loggerName shouldEqual "tech.ytsaurus.spyt.test"
    parsedEvent.level shouldEqual Some("ERROR")
    parsedEvent.thread shouldEqual Some("test-thread")
    parsedEvent.message shouldEqual "test message"
    parsedEvent.exceptionClass shouldEqual Some("java.lang.RuntimeException")
    parsedEvent.exceptionMessage shouldEqual Some("boom")
    parsedEvent.stack.getOrElse("") should include("java.lang.RuntimeException: boom")
  }

  it should "write timestamps in the format expected by worker log service" in {
    val eventTime = Instant.ofEpochMilli(eventMillis).atOffset(ZoneOffset.UTC)

    parsedEvent.dateTime shouldEqual WorkerLogBlock.formatter.format(eventTime)
    parsedEvent.date shouldEqual eventTime.toLocalDate
  }

  private def formatEvent(): String = withConfDirInClassLoader {
    val event = Log4jLogEvent.newBuilder()
      .setLoggerName("tech.ytsaurus.spyt.test")
      .setLevel(Level.ERROR)
      .setMessage(new SimpleMessage("test message"))
      .setThreadName("test-thread")
      .setThrown(new RuntimeException("boom"))
      .setTimeMillis(eventMillis)
      .build()

    configuration.getAppenders.get("console").getLayout.asInstanceOf[StringLayout].toSerializable(event)
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
