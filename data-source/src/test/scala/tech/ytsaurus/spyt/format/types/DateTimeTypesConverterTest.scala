package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.spyt.types.{Date32, Datetime64, Timestamp64}
import org.apache.spark.sql.spyt.types.Date32.{MAX_DATE32, MAX_DATE32_STR, MIN_DATE32, MIN_DATE32_STR}
import org.apache.spark.sql.spyt.types.Datetime64.{MAX_DATETIME64, MAX_DATETIME64_STR, MIN_DATETIME64, MIN_DATETIME64_STR}
import org.apache.spark.sql.spyt.types.Timestamp64.{MAX_TIMESTAMP64, MAX_TIMESTAMP64_STR, MIN_TIMESTAMP64, MIN_TIMESTAMP64_STR}
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.util.TimeZone


class DateTimeTypesConverterTest extends AnyFunSuite {

  /** DATE */

  testWithTimezones("dateToLong should convert date to number of days since epoch") {
    assert(dateToLong("1970-01-01") === 0)
    assert(dateToLong("1970-01-02") === 1)
    assert(dateToLong("2008-02-21") === 13930)
  }

  testWithTimezones("longToDate should convert number of days since epoch to date") {
    assert(longToDate(0) === "1970-01-01")
    assert(longToDate(1) === "1970-01-02")
    assert(longToDate(13930) === "2008-02-21")
  }

  testWithTimezones("dateToLong and longToDate should be inverses") {
    val dates = Seq("1970-01-01", "1999-12-31", "2008-02-21")
    dates.foreach { date =>
      assert(longToDate(dateToLong(date)) === date)
    }
  }

  /** DATETIME */

  testWithTimezones("datetimeToLong should convert timestamp to seconds since epoch") {
    assert(datetimeToLong("1970-01-01T00:00:00Z") === 0)
    assert(datetimeToLong("2019-02-09T13:41:11Z") === 1549719671L)
  }

  testWithTimezones("longToDatetime should convert seconds since epoch to timestamp") {
    assert(longToDatetime(0) === "1970-01-01T00:00:00Z")
    assert(longToDatetime(1549719671) === "2019-02-09T13:41:11Z")
  }

  testWithTimezones("datetimeToLong and longToDatetime should be inverses") {
    val timestamps = Seq("1970-01-01T00:00:00Z", "2019-02-09T13:41:11Z")
    timestamps.foreach { datetime =>
      assert(longToDatetime(datetimeToLong(datetime)) === datetime)
    }
  }

  /** TIMESTAMP */

  testWithTimezones("timestampToLong should convert timestamp string to microseconds since epoch") {
    assert(zonedTimestampToLong("1970-01-01T00:00:00.000000Z") === 0L)
    assert(zonedTimestampToLong("2019-02-09T13:41:11.000000Z") === 1549719671000000L)
    assert(zonedTimestampToLong("1970-01-01T00:00:00.123456Z") === 123456)
    assert(zonedTimestampToLong("2019-02-09T13:41:11.654321Z") === 1549719671654321L)
  }

  testWithTimezones("longToTimestamp should convert microseconds since epoch to timestamp string") {
    assert(longToZonedTimestamp(0L) === "1970-01-01T00:00:00.000000Z")
    assert(longToZonedTimestamp(1549719671000000L) === "2019-02-09T13:41:11.000000Z")
    assert(longToZonedTimestamp(123456) === "1970-01-01T00:00:00.123456Z")
    assert(longToZonedTimestamp(1549719671654321L) === "2019-02-09T13:41:11.654321Z")
  }

  testWithTimezones("timestampToLong and longToTimestamp should be inverses") {
    val timestamps = Seq("1970-01-01T00:00:00.000000Z", "2019-02-09T13:41:11.000000Z", "1970-01-01T00:00:00.123456Z", "2019-02-09T13:41:11.654321Z")
    timestamps.foreach { ts =>
      assert(longToZonedTimestamp(zonedTimestampToLong(ts)) === ts)
    }
  }

  testWithTimezones("convertUTCtoLocal should convert time from UTC to local") {
    val dateTimeStr = "1970-04-11T00:00:00Z"
    val expected = Timestamp.valueOf("1970-04-11 06:00:00.0")
    val result = convertUTCtoLocal(dateTimeStr, 6)
    assert(result === expected)
  }

  testWithTimezones("convertLocalToUTC should convert time from local to UTC") {
    val timestamp = Timestamp.valueOf("1970-04-11 06:00:00.0")
    val expected = "1970-04-11T00:00:00Z"
    val result = convertLocalToUTC(timestamp, 6)
    assert(result === expected)
  }

  /** WIDE TYPES */

  testWithTimezones("date32 converters"){
    assert(Date32(MIN_DATE32).toDate ===  LocalDate.parse(MIN_DATE32_STR))
    assert(Date32(MAX_DATE32).toDate === LocalDate.parse(MAX_DATE32_STR))
    assert(Date32(MIN_DATE32).toString === MIN_DATE32_STR)
    assert(Date32(MAX_DATE32).toString === MAX_DATE32_STR)
    assert(Date32(LocalDate.parse(MIN_DATE32_STR)).toInt === MIN_DATE32)
    assert(Date32(LocalDate.parse(MAX_DATE32_STR)).toInt === MAX_DATE32)
  }

  testWithTimezones("datetime64 converters"){
    assert(Datetime64(MIN_DATETIME64).toDatetime === toLocalDatetime(MIN_DATETIME64_STR))
    assert(Datetime64(MAX_DATETIME64).toDatetime === toLocalDatetime(MAX_DATETIME64_STR))
    assert(Datetime64(MIN_DATETIME64).toString === MIN_DATETIME64_STR)
    assert(Datetime64(MAX_DATETIME64).toString === MAX_DATETIME64_STR)
    assert(Datetime64(toLocalDatetime(MIN_DATETIME64_STR)).toLong === MIN_DATETIME64)
    assert(Datetime64(toLocalDatetime(MAX_DATETIME64_STR)).toLong === MAX_DATETIME64)
  }

  testWithTimezones("timestamp64 converters"){
    assert(Timestamp64(MIN_TIMESTAMP64).toString === MIN_TIMESTAMP64_STR)
    assert(Timestamp64(MAX_TIMESTAMP64).toString === MAX_TIMESTAMP64_STR)
    //TODO: investigate this test in different local timezones
    assert(Timestamp64(LocalDateTime.of(2000, 8, 14, 21, 50, 24, 123000)).toLong === 966289824000123L)
    assert(Timestamp64(MAX_TIMESTAMP64).toTimestamp.toString === "148107-12-31 23:59:59.999999")
    assert(Timestamp64(toLocalDatetime("-144168-01-01T00:00:00Z")).toString === MIN_TIMESTAMP64_STR)
  }

  private val DefaultTimezones =
    Seq("UTC", "America/New_York", "Europe/Moscow", "Asia/Tokyo", "Australia/Sydney")

  def testWithTimezones(name: String)(body: => Any): Unit = {
    test(name) {
      val originalTimezone = TimeZone.getDefault
      try {
        DefaultTimezones.foreach { timezoneId =>
          withClue(s"Timezone: $timezoneId") {
            TimeZone.setDefault(TimeZone.getTimeZone(timezoneId))
            body
          }
        }
      } finally {
        TimeZone.setDefault(originalTimezone)
      }
    }
  }
}


