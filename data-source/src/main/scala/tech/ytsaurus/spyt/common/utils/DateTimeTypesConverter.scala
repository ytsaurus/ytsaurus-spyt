package tech.ytsaurus.spyt.common.utils

import java.sql.{Date, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit}


object DateTimeTypesConverter {

  /** DATE */

  // "1970-01-01" -> 0L; "2008-02-21" -> 13930L
  def dateToLong(dateStr: String): Long = {
    val date = LocalDate.parse(dateStr)
    val epoch = LocalDate.of(1970, 1, 1)
    ChronoUnit.DAYS.between(epoch, date)
  }

  // 0L -> "1970-01-01"; 13930L -> "2008-02-21"
  def longToDate(days: Long): String = {
    val epoch = LocalDate.of(1970, 1, 1)
    val date = epoch.plusDays(days)
    date.toString
  }

  // Ex: convertDaysToDate(17410) -> Date "2017-08-03"
  def daysToDate(days: Long): Date = {
    val localDate = LocalDate.ofEpochDay(days)
    Date.valueOf(localDate)
  }

  /** DATETIME */

  // "1970-01-01T00:00:00Z" -> 0L; "2019-02-09T13:41:11Z" -> 1549719671L
  def datetimeToLong(timestampStr: String): Long = {
    val formatter = DateTimeFormatter.ISO_DATE_TIME
    val dateTime = ZonedDateTime.parse(timestampStr, formatter).withZoneSameInstant(java.time.ZoneOffset.UTC)
    dateTime.toEpochSecond
  }

  // 0 -> "1970-01-01T00:00:00Z"; 1549719671L -> "2019-02-09T13:41:11Z
  def longToDatetime(seconds: Long): String = {
    val dateTime = ZonedDateTime.ofInstant(java.time.Instant.ofEpochSecond(seconds), java.time.ZoneOffset.UTC)
    dateTime.format(DateTimeFormatter.ISO_DATE_TIME)
  }

  def localDateTimeToSeconds(localDateTime: LocalDateTime): Long = {
    val dateTime = localDateTime.atZone(java.time.ZoneOffset.UTC)
    dateTime.toInstant.toEpochMilli / 1000
  }

  // "+148108-01-01T02:59:59Z" -> LocalDatetime("+148108-01-01T02:59:59")
  def toLocalDatetime(dateTimeString: String): LocalDateTime = {
    val zonedDateTime = ZonedDateTime.parse(dateTimeString, DateTimeFormatter.ISO_DATE_TIME)
    zonedDateTime.toLocalDateTime
  }

  /** TIMESTAMP */

  def zonedTimestampToLong(timestampStr: String): Long = {
    val formatter = DateTimeFormatter.ISO_DATE_TIME
    val dateTime = ZonedDateTime.parse(timestampStr, formatter).withZoneSameInstant(java.time.ZoneOffset.UTC)
    dateTime.toEpochSecond * 1000000L + dateTime.get(ChronoField.MICRO_OF_SECOND)
  }

  def timestampToLong(timestamp: Timestamp): Long = {
    timestamp.getTime * 1000 + timestamp.getNanos / 1000
  }

  def longToZonedTimestamp(microseconds: Long): String = {
    val seconds = microseconds / 1000000L
    val microOfSecond = microseconds % 1000000L
    val dateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(seconds, 0), java.time.ZoneOffset.UTC)
    val formatted = dateTime.format(DateTimeFormatter.ISO_DATE_TIME)
    formatted.dropRight(1) + f".$microOfSecond%06dZ"
  }

  // Ex: convertUTCtoLocal("2019-02-09T13:41:11.654321Z", 3) -> Timestamp("2019-02-09 16:41:11.654321")
  def convertUTCtoLocal(str: String, zoneOffsetHour: Int): Timestamp = {
    val instant = Instant.parse(str)
    val offset = ZoneOffset.ofHours(zoneOffsetHour)
    val zoneId = ZoneId.of(offset.getId)
    val zonedDateTime = instant.atZone(zoneId)
    Timestamp.valueOf(zonedDateTime.toLocalDateTime)
  }

  // Timestamp("2019-02-09 16:41:11", 3) -> "2019-02-09T13:41:11Z"
  def convertLocalToUTC(timestamp: Timestamp, zoneOffset: Int): String = {
    val zoneOffsetAdjusted = ZoneOffset.ofHours(zoneOffset)
    val localDateTime = timestamp.toLocalDateTime.atZone(ZoneId.of(zoneOffsetAdjusted.getId))
    val utcDateTime = localDateTime.withZoneSameInstant(ZoneId.of("UTC"))
    utcDateTime.toInstant.toString
  }

  /** Other */

  // Ex: 3 for MSK
  def getUtcHoursOffset: Int = {
    val zoneId = ZoneId.systemDefault()
    val now = ZonedDateTime.now(zoneId)
    val offsetInSeconds = now.getOffset.getTotalSeconds
    offsetInSeconds / 3600
  }
}
