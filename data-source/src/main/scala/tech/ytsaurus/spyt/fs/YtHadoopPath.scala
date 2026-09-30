package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.Path
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.config.ConfigEntry.{fromJsonTyped, toJsonTyped}
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import scala.util.Try

case class YtTableMeta(
  rowCount: Long = 0,
  size: Long = 1L,
  modificationTime: Long = 0L,
  optimizeMode: OptimizeMode = OptimizeMode.Scan,
  isDynamic: Boolean = false,
  fullReadAllowed: Boolean = true,
  schemaIdOpt: Option[String] = None,
  securityTags: Seq[String] = Nil) extends Serializable {

  def approximateRowSize: Long = if (rowCount == 0) 0 else (size + rowCount - 1) / rowCount
}

case class YtHadoopPath(ypath: YPathEnriched, meta: YtTableMeta)
  extends Path(ypath.toPath, YtHadoopPath.toFileName(meta)) with Serializable {

  def toStringPath: String = ypath.toStringPath

  def toYPath: YPath = ypath.toYPath
}

object YtHadoopPath {
  private def toFileName(meta: YtTableMeta): String = {
    import meta._
    List(
      rowCount,
      size,
      modificationTime,
      optimizeMode.name,
      isDynamic,
      fullReadAllowed,
      schemaIdOpt.getOrElse("None"),
      Base64.getUrlEncoder.withoutPadding().encodeToString(toJsonTyped(securityTags).getBytes(UTF_8))
    ).mkString("_")
  }

  private def tryDeserialize(path: Path): Option[YtHadoopPath] = {
    Try {
      val (rowCountStr :: sizeStr :: modificationTimeStr :: optimizeModeStr ::
        isDynamicStr :: fullReadAllowedStr :: schemaIdOptStr :: extra) = path.getName.trim.split("_", 8).toList
      val rowCount = rowCountStr.trim.toLong
      val size = sizeStr.trim.toLong
      val modificationTime = modificationTimeStr.trim.toLong
      val optimizeMode = OptimizeMode.fromName(optimizeModeStr.trim)
      val isDynamic = isDynamicStr.trim.toBoolean
      val fullReadAllowed = fullReadAllowedStr.trim.toBoolean
      val schemaIdOpt = schemaIdOptStr.trim match {
        case "None" => None
        case s => Some(s)
      }
      val securityTags = extra.headOption.map { encoded =>
        fromJsonTyped[Seq[String]](new String(Base64.getUrlDecoder.decode(encoded), UTF_8))
      }.getOrElse(Nil)
      YtHadoopPath(
        YPathEnriched.fromPath(path.getParent),
        YtTableMeta(rowCount, size, modificationTime, optimizeMode, isDynamic, fullReadAllowed, schemaIdOpt, securityTags))
    }.toOption
  }

  def fromPath(path: Path): Path = {
    path match {
      case yp: YtHadoopPath => yp
      case p => YtHadoopPath.tryDeserialize(p).getOrElse(p)
    }
  }
}
