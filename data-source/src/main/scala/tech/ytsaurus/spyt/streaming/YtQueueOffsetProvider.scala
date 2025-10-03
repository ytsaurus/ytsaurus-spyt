package tech.ytsaurus.spyt.streaming

import tech.ytsaurus.client.CompoundClient

import scala.util.Try

object YtQueueOffsetProvider extends YtQueueOffsetProviderTrait {
  override def getMaxOffset(cluster: String, queuePath: String)(implicit client: CompoundClient): Try[YtQueueOffset] = {
    YtQueueOffset.getMaxOffset(cluster, queuePath)
  }

  override def getCurrentOffset(cluster: String, consumerPath: String, queuePath: String)(implicit client: CompoundClient): YtQueueOffset = {
    YtQueueOffset.getCurrentOffset(cluster, consumerPath, queuePath)
  }

  override def advance(consumerPath: String, newOffset: YtQueueOffset, lastCommittedOffset: YtQueueOffset, maxOffset: Option[YtQueueOffset])
                      (implicit client: CompoundClient): Option[YtQueueOffset] = {
    YtQueueOffset.advance(consumerPath, newOffset, lastCommittedOffset, maxOffset)
  }
}
