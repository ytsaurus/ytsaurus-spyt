package tech.ytsaurus.spyt.wrapper.operation

import tech.ytsaurus.ysontree.YTreeNode

case class BriefProgress(
                          running: Int,
                          total: Int,
                          blocked: Int,
                          aborted: Int,
                          lost: Int,
                          pending: Int,
                          completed: Int,
                          failed: Int,
                          invalidated: Int,
                          suspended: Int
                        ) {
  override def toString: String = {
    s"""BriefProgress(
       |  running     = $running,
       |  total       = $total,
       |  blocked     = $blocked,
       |  aborted     = $aborted,
       |  lost        = $lost,
       |  pending     = $pending,
       |  completed   = $completed,
       |  failed      = $failed,
       |  invalidated = $invalidated,
       |  suspended   = $suspended
       |)""".stripMargin
  }
}

object BriefProgress {
  def fromOperationSpec(spec: YTreeNode): Option[BriefProgress] = {
    val specMap = spec.mapNode()
    if (
      specMap.containsKey("brief_progress") &&
        specMap.getMap("brief_progress").mapNode().containsKey("jobs")
    ) {
      val jobs = specMap.getMap("brief_progress").getMap("jobs")
      try {
        def getField(name: String): Int = {
          try jobs.getInt(name)
          catch {
            case ex: Throwable =>
              throw new IllegalArgumentException(s"Failed to parse `$name` in jobs map: ${ex.getMessage}", ex)
          }
        }

        Some(BriefProgress(
          running     = getField("running"),
          total       = getField("total"),
          blocked     = getField("blocked"),
          aborted     = getField("aborted"),
          lost        = getField("lost"),
          pending     = getField("pending"),
          completed   = getField("completed"),
          failed      = getField("failed"),
          invalidated = getField("invalidated"),
          suspended   = getField("suspended")
        ))
      }
      catch {
        case ex: Throwable => throw ex
      }
    } else {
      None
    }
  }
}

