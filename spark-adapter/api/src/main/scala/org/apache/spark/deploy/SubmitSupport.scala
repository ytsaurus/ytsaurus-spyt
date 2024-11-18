package org.apache.spark.deploy

import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}

import java.util.ServiceLoader

trait SubmitSupport {
  val YTSAURUS_IS_PYTHON: ConfigEntry[Boolean]
  val YTSAURUS_IS_PYTHON_BINARY: ConfigEntry[Boolean]
  val YTSAURUS_POOL: OptionalConfigEntry[String]
  val YTSAURUS_PYTHON_BINARY_ENTRY_POINT: OptionalConfigEntry[String]
  val YTSAURUS_PYTHON_EXECUTABLE: OptionalConfigEntry[String]
  def pythonBinaryWrapperPath(spytHome: String): String
}

object SubmitSupport {
  lazy val instance: SubmitSupport = ServiceLoader.load(classOf[SubmitSupport]).findFirst().get()
}
