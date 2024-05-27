
package org.apache.spark.deploy.ytsaurus

import org.apache.spark.SparkConf
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

object YTsaurusUtils {

  val URL_PREFIX = "ytsaurus://"

  def parseMasterUrl(masterURL: String): String = {
    masterURL.substring(URL_PREFIX.length)
  }

  def userAndToken(conf: SparkConf): (String, String) = {
    val user = sys.env.get("YT_SECURE_VAULT_YT_USER").orElse(conf.getOption("spark.hadoop.yt.user")).orNull
    val token = sys.env.get("YT_SECURE_VAULT_YT_TOKEN").orElse(conf.getOption("spark.hadoop.yt.token")).orNull
    if (user == null || token == null) {
      val auth = YTsaurusClientAuth.loadUserAndTokenFromEnvironment()
      (auth.getUser.orElseThrow(), auth.getToken.orElseThrow())
    } else {
      (user, token)
    }
  }
}
