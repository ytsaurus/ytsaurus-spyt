package tech.ytsaurus.spyt.format.columnar

/** Shared deterministic cleanup operations for the columnar execution boundary.
 *
 * Use these helpers when releasing Arrow roots, evaluators, and allocators so cleanup failures do not hide
 * the original evaluation error or prevent subsequent resources from being closed.
 */
private[columnar] object ColumnarResources {

  /** Attaches a cleanup failure without rejecting providers that reuse the original exception instance. */
  def suppress(error: Throwable, cleanup: Throwable): Unit = {
    if (error ne cleanup) error.addSuppressed(cleanup)
  }

  /** Evaluates a body and closes its resource on success or failure, suppressing cleanup errors onto body errors.
   *
   * The returned value must not depend on the resource remaining open unless it retains its own references.
   */
  def using[T](resource: AutoCloseable)(body: => T): T = {
    val result = withCleanupOnFailure(resource.close())(body)
    resource.close()
    result
  }

  /** Runs deferred cleanup only on failure, retaining the original error and suppressing cleanup errors.
   *
   * Use while constructing an owned result; on success its resources remain open for the caller.
   */
  def withCleanupOnFailure[T](cleanup: => Unit)(body: => T): T = {
    try {
      body
    } catch {
      case error: Throwable =>
        try {
          cleanup
        } catch {
          case cleanupError: Throwable => suppress(error, cleanupError)
        }
        throw error
    }
  }

  /** Closes every supplied resource in order, then throws the first failure with later failures suppressed.
   *
   * Callers should order output batches before their evaluators and allocators.
   */
  def closeAll(resources: Iterator[AutoCloseable]): Unit = {
    var failure: Option[Throwable] = None
    resources.foreach { resource =>
      try {
        resource.close()
      } catch {
        case error: Throwable => failure match {
          case Some(first) => suppress(first, error)
          case None => failure = Some(error)
        }
      }
    }
    failure.foreach(throw _)
  }
}
