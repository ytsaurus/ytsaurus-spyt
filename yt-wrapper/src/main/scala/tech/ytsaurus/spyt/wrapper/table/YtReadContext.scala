package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.client.CompoundClient

/**
 * Context for table reading operations with YT tables.
 */
class YtReadContext(val yt: CompoundClient, val settings: YtReadSettings)

object YtReadContext {

  def apply(yt: CompoundClient, settings: YtReadSettings): YtReadContext =
    new YtReadContext(yt, settings)

  def withContext[T](yt: CompoundClient, settings: YtReadSettings)
                    (block: YtReadContext => T): T = {
    val ctx = YtReadContext(yt, settings)
    try {
      block(ctx)
    } finally {
    }
  }
}
