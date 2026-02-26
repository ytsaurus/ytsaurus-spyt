package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.SaveMode
import tech.ytsaurus.spyt.format.YtOutputCommitProtocol

class YTsaurusCommitProtocolSupport extends CommitProtocolSupport {

  override def setSaveMode(mode: SaveMode): Unit = {
    YtOutputCommitProtocol.saveModeTL.set(mode)
  }

  override def clearSaveMode(): Unit = {
    YtOutputCommitProtocol.saveModeTL.remove()
  }
}
