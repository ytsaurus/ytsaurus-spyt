package spyt

import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode, YTreeTextSerializer}
import tech.ytsaurus.client.YTsaurusClient
import spyt.SparkPaths._

import scala.annotation.tailrec

sealed trait YsonableConfig {
  def toYson: String = {
    YTreeTextSerializer.serialize(toYTree)
  }

  def toYTree: YTreeNode = {
    toYson(new YTreeBuilder()).build()
  }

  def toYson(builder: YTreeBuilder): YTreeBuilder = {
    this.getClass.getDeclaredFields.foldLeft(builder.beginMap()) {
      case (res, nextField) =>
        nextField.setAccessible(true)
        YsonableConfig.toYson(nextField.get(this), res.key(nextField.getName))
    }.endMap()
  }

  def resolveSymlinks(implicit yt: YTsaurusClient): YsonableConfig = this
}

object YsonableConfig {
  @tailrec
  private def toYson(value: Any, builder: YTreeBuilder): YTreeBuilder = {
    value match {
      case Some(value) => toYson(value, builder)
      case None => builder.entity()
      case m: Map[_, _] => m.foldLeft(builder.beginMap()) { case (res, (k: String, v)) => res.key(k).value(v) }.endMap()
      case ss: Seq[_] => ss.foldLeft(builder.beginList()) { case (res, next) => res.value(next) }.endList()
      case c: YsonableConfig => c.toYson(builder)
      case any => builder.value(any)
    }
  }

  def toYson(value: Any): String = {
    YTreeTextSerializer.serialize(toYTree(value))
  }

  def toYTree(value: Any): YTreeNode = {
    val builder = new YTreeBuilder()
    toYson(value, builder)
    builder.build()
  }
}

case class SparkLaunchConfig(spark_yt_base_path: String,
                             file_paths: Seq[String],
                             spark_conf: Map[String, String],
                             enablers: SpytEnablers = SpytEnablers(),
                             ytserver_proxy_path: Option[String] = None,
                             layer_paths: Seq[String] = SparkLaunchConfig.defaultLayers,
                             squashfs_layer_paths: Seq[String] = SparkLaunchConfig.squashFsLayers,
                             environment: Map[String, String] = Map.empty) extends YsonableConfig {
  override def resolveSymlinks(implicit yt: YTsaurusClient): YsonableConfig = {
    import SparkLaunchConfig._
    if (ytserver_proxy_path.isEmpty) {
      copy(ytserver_proxy_path = Some(resolveSymlink(defaultYtServerProxyPath)))
    } else this
  }
}

case class SpytEnablers(enable_byop: Boolean = true,
                        enable_mtn: Boolean = true,
                        enable_yt_metrics: Boolean = true,
                        enable_tcp_proxy: Boolean = true,
                        enable_squashfs: Boolean = true) extends YsonableConfig {
  override def toYson(builder: YTreeBuilder): YTreeBuilder = {
    builder.beginMap()
      .key("spark.hadoop.yt.byop.enabled").value(enable_byop)
      .key("spark.hadoop.yt.mtn.enabled").value(enable_mtn)
      .key("spark.ytsaurus.metrics.enabled").value(enable_yt_metrics)
      .key("spark.hadoop.yt.tcpProxy.enabled").value(enable_tcp_proxy)
      .key("spark.ytsaurus.squashfs.enabled").value(enable_squashfs)
      .endMap()
  }
}

object SparkLaunchConfig {
  val defaultLayers = Seq(
    s"$sparkYtDeltaLayerPath/layer_with_unify_agent.tar.gz",
    s"$ytPortoDeltaLayersPath/jdk/layer_with_jdk_lastest.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python312_jammy_v001.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python311_focal_v002.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python39_focal_v002.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python38_focal_v002.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python37_focal_yandexyt0131.tar.gz",
    s"$ytPortoBaseLayersPath/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz"
  )

  val squashFsLayers = Seq(
    s"$sparkYtSquashfsLayerPath/layer_with_unify_agent.squashfs",
    s"$sparkYtSquashfsLayerPath/jdk/layer_with_jdk_latest.squashfs",
    s"$sparkYtSquashfsLayerPath/python/layer_with_python312_focal_v002.squashfs",
    s"$sparkYtSquashfsLayerPath/python/layer_with_python311_focal_v002.squashfs",
    s"$sparkYtSquashfsLayerPath/python/layer_with_python39_focal_v002.squashfs",
    s"$sparkYtSquashfsLayerPath/python/layer_with_python38_focal_v002.squashfs",
    s"$sparkYtSquashfsLayerPath/python/layer_with_python37_focal_yandexyt0131.squashfs",
    s"$ytPortoBaseLayersPath/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz"
  )

  def resolveSymlink(symlink: String)(implicit yt: YTsaurusClient): String = {
    yt.getNode(s"$symlink&/@target_path").join().stringValue()
  }
}
