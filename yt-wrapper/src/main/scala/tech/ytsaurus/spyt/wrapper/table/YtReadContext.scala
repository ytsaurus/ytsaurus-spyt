package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.GUID

/**
 * Context for table reading operations with YT tables.
 */
class YtReadContext(val yt: CompoundClient, val settings: YtReadSettings, val requestId: GUID)

object YtReadContext {

  def apply(yt: CompoundClient, settings: YtReadSettings): YtReadContext =
    new YtReadContext(yt, settings, GUID.create())

  def withContext[T](yt: CompoundClient, settings: YtReadSettings)
                    (block: YtReadContext => T): T = {
    val ctx = YtReadContext(yt, settings)
    try {
      block(ctx)
    } finally {
    }
  }
}
