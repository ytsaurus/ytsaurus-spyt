
package org.apache.spark.deploy.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmit
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

object YTsaurusUtils {

  val URL_PREFIX = "ytsaurus://"

  def parseMasterUrl(masterURL: String): String = {
    masterURL.substring(URL_PREFIX.length)
  }

  def getToken(conf: SparkConf): String = {
    sys.env.get("YT_SECURE_VAULT_YT_TOKEN")
      .orElse(conf.getOption("spark.hadoop.yt.token"))
      .getOrElse(YTsaurusClientAuth.loadUserAndTokenFromEnvironment().getToken.orElseThrow())
  }

  // Increasing visibility of SparkSubmit.isShell method
  def isShell(res: String): Boolean = SparkSubmit.isShell(res)

  def pythonBinaryWrapperPath(spytHome: String): String = {
    s"$spytHome/bin/python-binary-wrapper.sh"
  }
}
