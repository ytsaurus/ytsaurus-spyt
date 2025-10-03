package tech.ytsaurus.spyt.streaming

import tech.ytsaurus.client.CompoundClient

import scala.util.Try

trait YtQueueOffsetProviderTrait {
  def getMaxOffset(cluster: String, queuePath: String)(implicit client: CompoundClient): Try[YtQueueOffset]

  def getCurrentOffset(cluster: String, consumerPath: String, queuePath: String)
                      (implicit client: CompoundClient): YtQueueOffset

  def advance(consumerPath: String, newOffset: YtQueueOffset, lastCommittedOffset: YtQueueOffset, maxOffset: Option[YtQueueOffset])
             (implicit client: CompoundClient): Option[YtQueueOffset]
}