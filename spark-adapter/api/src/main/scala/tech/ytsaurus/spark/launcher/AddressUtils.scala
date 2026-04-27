package tech.ytsaurus.spark.launcher

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import tech.ytsaurus.spyt.logging.Logging

import java.io.{File, PrintWriter}

object AddressUtils extends Logging {
  def writeAddressToFile(name: String,
                         host: String,
                         port: Int,
                         webUiPort: Option[Int],
                         webUiUrl: Option[String],
                         restPort: Option[Int]): Unit = {
    logInfo(s"Writing address to file: $port, $webUiPort, $restPort")

    val mapper = new ObjectMapper() with ClassTagExtensions
    mapper.registerModule(DefaultScalaModule)
    val addressString = mapper.writeValueAsString(Map(
      "host" -> host,
      "port" -> port,
      "webUiPort" -> webUiPort,
      "webUiUrl" -> webUiUrl,
      "restPort" -> restPort
    ))

    val pw = new PrintWriter(new File(s"${name}_address"))
    try {
      pw.write(addressString)
      require(new File(s"${name}_address_success").createNewFile())
    } finally {
      pw.close()
    }
  }

}
