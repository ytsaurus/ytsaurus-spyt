package tech.ytsaurus.spyt.format

import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.ysontree.YTree

trait DescendingSortOrderSupport {
  def withDescendingSortOrder[T](block: => T)(implicit yt: CompoundClient): T = {
    yt.setNode("//sys/@config/enable_descending_sort_order", YTree.booleanNode(true)).join()
    try {
      block
    } finally {
      yt.setNode("//sys/@config/enable_descending_sort_order", YTree.booleanNode(false)).join()
    }
  }
}
