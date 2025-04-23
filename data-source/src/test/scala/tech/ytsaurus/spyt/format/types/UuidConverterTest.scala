package tech.ytsaurus.spyt.format.types

import org.scalatest.funsuite.AnyFunSuite
import tech.ytsaurus.spyt.common.utils.UuidUtils

class UuidConverterTest extends AnyFunSuite {
  test("convert UUID to bytes and back to UUID")  {
    val uuid = "4b336d01-6576-4c7a-5436-4f6847644642"
    val encodedBytes = UuidUtils.uuidToBytes(uuid)
    val expectedBytes = "\u0001m3KvezLT6OhGdFB".getBytes

    assert(encodedBytes.length === 16)
    assert(encodedBytes === expectedBytes)

    val decodedUuid = UuidUtils.bytesToUuid(encodedBytes)
    assert(decodedUuid === uuid)
  }
}
