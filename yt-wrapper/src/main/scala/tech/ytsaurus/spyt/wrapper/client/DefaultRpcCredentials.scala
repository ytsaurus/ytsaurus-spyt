package tech.ytsaurus.spyt.wrapper.client

import java.nio.file.{Files, Path, Paths}
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

import java.util.Objects

object DefaultRpcCredentials {
  def token: String = {
    sys.env.getOrElse("YT_TOKEN", readFileFromHome(".yt", "token"))
  }

  def tokenUser(token: String): String = sys.env.getOrElse(
    "YT_USER", if (Objects.requireNonNullElse(token, "").isEmpty) System.getProperty("user.name") else null
  )

  def credentials: YTsaurusClientAuth = {
    val token = this.token
    YTsaurusClientAuth.builder().setUser(tokenUser(token)).setToken(token).build()
  }

  private def readFileFromHome(path: String*): String = {
    readFile(Paths.get(System.getProperty("user.home"), path: _*))
  }

  private def readFile(path: Path): String = {
    val reader = Files.newBufferedReader(path)
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }
}
