
package org.apache.spark.deploy.ytsaurus

import org.apache.spark.SparkConf
import tech.ytsaurus.client.rpc.YTsaurusClientAuth

object YTsaurusUtils {

  val URL_PREFIX = "ytsaurus://"

  def parseMasterUrl(masterURL: String): String = {
    masterURL.substring(URL_PREFIX.length)
  }

  def token(conf: SparkConf): String = {
    val token = sys.env.get("YT_SECURE_VAULT_YT_TOKEN").orElse(conf.getOption("spark.hadoop.yt.token")).orNull
    if (token == null) {
      YTsaurusClientAuth.loadUserAndTokenFromEnvironment().getToken.orElseThrow()
    } else {
      token
    }
  }
}
