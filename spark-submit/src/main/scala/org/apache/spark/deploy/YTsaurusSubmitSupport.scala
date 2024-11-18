package org.apache.spark.deploy

import org.apache.spark.deploy.ytsaurus.{Config, YTsaurusUtils}
import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}

class YTsaurusSubmitSupport extends SubmitSupport {
  override val YTSAURUS_IS_PYTHON: ConfigEntry[Boolean] = Config.YTSAURUS_IS_PYTHON
  override val YTSAURUS_IS_PYTHON_BINARY: ConfigEntry[Boolean] = Config.YTSAURUS_IS_PYTHON_BINARY
  override val YTSAURUS_POOL: OptionalConfigEntry[String] = Config.YTSAURUS_POOL
  override val YTSAURUS_PYTHON_BINARY_ENTRY_POINT: OptionalConfigEntry[String] =
    Config.YTSAURUS_PYTHON_BINARY_ENTRY_POINT
  override val YTSAURUS_PYTHON_EXECUTABLE: OptionalConfigEntry[String] =
    Config.YTSAURUS_PYTHON_EXECUTABLE

  def pythonBinaryWrapperPath(spytHome: String): String = {
    YTsaurusUtils.pythonBinaryWrapperPath(sys.env("SPYT_ROOT"))
  }
}
