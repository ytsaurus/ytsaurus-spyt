package tech.ytsaurus.spyt.wrapper.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MasterWrapperClientTest extends AnyFlatSpec with Matchers {

  behavior of "ByopDiscoveryClientTest"

  it should "parseByopEnabled" in {
    def text(enabled: Boolean) =
      s"""
        |{
        |"byop_enabled": $enabled
        |}
        |""".stripMargin

    MasterWrapperClient.parseByopEnabled(text(true)) shouldEqual Right(true)
    MasterWrapperClient.parseByopEnabled(text(false)) shouldEqual Right(false)
    MasterWrapperClient.parseByopEnabled("{}").isLeft shouldEqual true
    MasterWrapperClient.parseByopEnabled("}").isLeft shouldEqual true
  }

}
