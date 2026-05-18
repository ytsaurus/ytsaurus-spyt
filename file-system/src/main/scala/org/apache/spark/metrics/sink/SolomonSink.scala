package org.apache.spark.metrics.sink

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager
import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.metrics.{ReporterConfig, SolomonConfig, SolomonReporter}

import java.util.Properties

private[this] case class SolomonSink(props: Properties, registry: MetricRegistry, securityMgr: SecurityManager)
  extends Sink {

  private val log = LoggerFactory.getLogger(SolomonSink.getClass)
  private val reporter: Option[SolomonReporter] = for {
    solomonConfig <- Some(SolomonConfig.read(props))
    reporterConfig <- Some(ReporterConfig.read(props))
    reporter <- try {
      SolomonReporter.tryCreateSolomonReporter(registry, solomonConfig, reporterConfig, props)
    } catch {
      case ex: RuntimeException =>
        log.error("Failed to start metrics reporting", ex)
        None
    }
  } yield reporter

  override def start(): Unit = reporter match {
    case None =>
      log.info("Solomon reporter is disabled")
    case Some(r) =>
      r.start()
  }

  override def stop(): Unit = reporter.foreach { r =>
    log.info(s"Stopping SolomonSink")
    r.stop()
  }

  override def report(): Unit = reporter.foreach { r =>
    log.debug(s"Sending report")
    r.report()
  }
}
