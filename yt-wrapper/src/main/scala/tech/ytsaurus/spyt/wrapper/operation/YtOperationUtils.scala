package tech.ytsaurus.spyt.wrapper.operation

import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.GetOperation
import tech.ytsaurus.core.GUID

trait YtOperationUtils {
  def fetchBriefProgress(operationId: GUID)
                      (implicit yt: CompoundClient): Option[BriefProgress] = {
    val request = GetOperation.builder().setOperationId(operationId).build()
    val opSpec = yt.getOperation(request).join()
    BriefProgress.fromOperationSpec(opSpec)
  }
}
