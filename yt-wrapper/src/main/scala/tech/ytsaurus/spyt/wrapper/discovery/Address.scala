package tech.ytsaurus.spyt.wrapper.discovery

import tech.ytsaurus.spyt.HostAndPort

import java.net.URI

case class Address(host: String, port: Int, webUiPort: Option[Int], webUiUrl: Option[String], restPort: Option[Int]) {
  def hostAndPort: HostAndPort = HostAndPort(host, port)

  def webUiHostAndPort: HostAndPort = HostAndPort(host, webUiPort.get)

  def webUiUri: URI = URI.create(webUiUrl.get)

  def restHostAndPort: HostAndPort = HostAndPort(host, restPort.get)
}

object Address {
  def apply(hostAndPort: HostAndPort, webUiHostAndPort: HostAndPort, webUiUrl: URI, restHostAndPort: HostAndPort): Address = {
    Address(hostAndPort.host, hostAndPort.port, Some(webUiHostAndPort.port), Some(webUiUrl.toString), Some(restHostAndPort.port))
  }
}