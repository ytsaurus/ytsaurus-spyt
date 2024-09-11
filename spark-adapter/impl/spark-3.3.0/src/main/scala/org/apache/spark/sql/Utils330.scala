package org.apache.spark.sql

import org.apache.log4j.bridge.LogEventAdapter
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Category, Level}
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.SimpleMessage
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources.Filter

object Utils330 {

  def filterToPredicate(f: Filter): Predicate = f.toV2

  def createLoggingEvent(fqnOfCategoryClass: String, logger: Category,
                         timeStamp: Long, level: Level,
                         message: String, throwable: Throwable): LoggingEvent = {
    val logEvent = Log4jLogEvent.newBuilder()
      .setLoggerFqcn(fqnOfCategoryClass)
      .setLoggerName(logger.getName)
      .setTimeMillis(timeStamp)
      .setLevel(org.apache.logging.log4j.Level.toLevel(level.toString))
      .setMessage(new SimpleMessage(message))
      .setThrown(throwable)
      .build()
    new LogEventAdapter(logEvent)
  }

}
