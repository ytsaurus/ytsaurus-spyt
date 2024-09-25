package tech.ytsaurus.spyt.wrapper.client

import java.nio.file.{Files, Path, Paths}

object DefaultRpcCredentials {
  def token: String = {
    sys.env.getOrElse("YT_TOKEN", readFileFromHome(".yt", "token"))
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
