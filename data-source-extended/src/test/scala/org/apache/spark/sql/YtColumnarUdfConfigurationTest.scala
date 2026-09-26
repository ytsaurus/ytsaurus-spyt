package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.SparkVersionUtils
import tech.ytsaurus.spyt.format.YtSparkExtensions
import tech.ytsaurus.spyt.format.columnar.{RegisterColumnarFunction, YtColumnarUdfRule}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.ColumnarUdfEnabled
import tech.ytsaurus.spyt.test.LocalSpark

/** Checks startup opt-in and ensures disabled sessions bypass columnar handling. */
class YtColumnarUdfConfigurationTest extends AnyFlatSpec with LocalSpark with Matchers {

  /** Creates a fresh Spark context so each case exercises a distinct startup configuration. */
  override def reinstantiateSparkSession: Boolean = true

  it should "delegate named SQL parameters through the enabled parser" in {
    assume(!SparkVersionUtils.lessThan("4.1.0"), "Columnar parsing requires Spark 4.1.0 or later")
    withSparkSession(Map(ColumnarUdfEnabled.name -> "true")) { session =>
      session.sql("SELECT :value + 1", Map("value" -> 7)).collect().toSeq shouldBe Seq(Row(8))
    }
  }

  for (setting <- Seq(None, Some("false"), Some("true"))) {
    it should s"select columnar extensions with startup setting $setting" in {
      assume(!setting.contains("true") || !SparkVersionUtils.lessThan("4.1.0"))
      withSparkSession(setting.map(ColumnarUdfEnabled.name -> _).toMap) { session =>
        val extensions = new SparkSessionExtensions
        new YtSparkExtensions().apply(extensions)
        val delegate: ParserInterface = session.sessionState.sqlParser
        val parser = extensions.buildParser(session, delegate)
        val rules = extensions.buildColumnarRules(session)
        val enabled = setting.contains("true")
        rules.exists(_.isInstanceOf[YtColumnarUdfRule]) shouldBe true
        if (enabled) {
          parser.parsePlan("REGISTER COLUMNAR FUNCTION example AS 'example.Provider'") shouldBe
            RegisterColumnarFunction("example", "example.Provider", Map.empty)
        } else {
          parser should be theSameInstanceAs delegate
          intercept[org.apache.spark.sql.catalyst.parser.ParseException] {
            parser.parsePlan("REGISTER COLUMNAR FUNCTION example AS 'example.Provider'")
          }
          val plan = session.range(1).queryExecution.executedPlan
          rules.foreach(_.preColumnarTransitions(plan) should be theSameInstanceAs plan)
        }
      }
    }
  }
}
