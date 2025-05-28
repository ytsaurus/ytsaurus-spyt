package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle}
import tech.ytsaurus.client.request.{ShuffleHandle => YTsaurusShuffleHandle}

case class CompoundShuffleHandle[K, V, C](
  baseHandle: BaseShuffleHandle[K, V, C],
  ytHandle: YTsaurusShuffleHandle,
  partitionCount: Int
) extends ShuffleHandle(baseHandle.shuffleId)
