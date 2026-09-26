package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Evaluates a partition with lazy call-local evaluators and task-scoped Arrow resources.
 *
 * Created by YtColumnarUdfExec on executors. Results remain valid until closed, the next batch is requested,
 * or the task completes; probing hasNext never releases the current result.
 */
private[columnar] class ColumnarFunctionIterator(
  input: Iterator[ColumnarBatch],
  expressions: Seq[Expression],
  passThroughOrdinals: Seq[Int],
  passThroughSchemaJson: Option[String],
  artifactDirectory: String) extends Iterator[ColumnarBatch] {

  private val partitionAllocator = new RootAllocator()
  private val evaluators = mutable.ArrayBuffer.empty[ColumnarEvaluator]
  private var current: Option[ColumnarBatch] = None
  private var closed = false
  private var availability: Option[Boolean] = None
  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private val context = new ExecutorColumnarFunctionContext(partitionAllocator, artifactDirectory)

  private val prepared: Seq[Either[Int, PreparedColumnarFunction]] = ColumnarResources.withCleanupOnFailure(close()) {
    val passThroughPositions = passThroughOrdinals.iterator.zipWithIndex.toMap
    expressions.map(expression => YtColumnarUdfExec.unalias(expression) match {
      case reference: BoundReference => Left(passThroughPositions(reference.ordinal))
      case function: YtColumnarFunction => Right(new PreparedColumnarFunction(function, context, evaluators += _))
      case other => throw new IllegalArgumentException(s"Unsupported columnar expression: $other")
    })
  }
  private val passThroughSchema = ColumnarResources.withCleanupOnFailure(close()) {
    passThroughSchemaJson.map(Schema.fromJSON)
  }

  /** Releases the previous result before evaluating the next batch. */
  private def release(): Unit = {
    val previous = current
    current = None
    previous.foreach(_.close())
  }

  /** Closes output roots, evaluators, then the allocator once, including when the task ends early. */
  private def close(): Unit = if (!closed) {
    closed = true
    val resources = current.iterator ++ evaluators.iterator ++ Iterator.single(partitionAllocator)
    current = None
    try {
      ColumnarResources.closeAll(resources)
    } finally {
      evaluators.clear()
    }
  }

  /** Caches upstream availability without releasing the last returned batch, even on exhaustion. */
  override def hasNext: Boolean = ColumnarResources.withCleanupOnFailure(close()) {
    if (closed) false else availability.getOrElse {
      val more = input.hasNext
      availability = Some(more)
      more
    }
  }

  /** Evaluates one source batch and validates each owned plugin result before returning it.
   *
   * Input roots are borrowed only for the evaluate call. Returned roots must own any retained buffers,
   * match the registered schema and row count, and remain valid until the result batch is released.
   */
  override def next(): ColumnarBatch = ColumnarResources.withCleanupOnFailure(close()) {
    if (!hasNext) throw new NoSuchElementException("No more columnar function batches")
    release()
    availability = None
    val batch = evaluateBatch(input.next())
    current = Some(batch)
    batch
  }

  /** Transfers successfully evaluated roots to the returned batch; closes partial results on failure. */
  private def evaluateBatch(source: ColumnarBatch): ColumnarBatch = {
    val ownedRoots = mutable.ArrayBuffer.empty[VectorSchemaRoot]
    ColumnarResources.withCleanupOnFailure(ColumnarResources.closeAll(ownedRoots.iterator)) {
      val arguments = new ColumnarArrowInput(source, partitionAllocator)
      ColumnarResources.using(arguments) {
        val passThroughRoot = passThroughSchema.map(arguments.create(passThroughOrdinals, _))
        ownedRoots ++= passThroughRoot
        val columns = prepared.iterator.map {
          case Left(position) => YtColumnarUdfExec.borrowedColumn(passThroughRoot.get.getVector(position))
          case Right(call) => evaluateFunction(call, arguments, source.numRows(), ownedRoots)
        }.toArray[ColumnVector]
        new ColumnarFunctionBatch(columns, source.numRows(), ownedRoots)
      }
    }
  }

  /** Borrows arguments for one call and retains its owned output before validating it. */
  private def evaluateFunction(
    call: PreparedColumnarFunction,
    arguments: ColumnarArrowInput,
    rowCount: Int,
    ownedRoots: mutable.Buffer[VectorSchemaRoot]): ColumnVector = {
    val evaluator = call.evaluator
    val borrowed = arguments.create(call.ordinals, call.function.inputSchema)
    ColumnarResources.using(borrowed) {
      val result = evaluator.evaluate(borrowed)
      require(result != null, s"${call.function.prettyName} returned no Arrow batch")
      require(result ne borrowed, "A columnar function must return an owned output batch")
      ownedRoots += result
      validateResult(call, result, rowCount)
      YtColumnarUdfExec.borrowedColumn(result.getVector(0))
    }
  }

  /** Checks result metadata before exposing the owned vectors to Spark. */
  private def validateResult(call: PreparedColumnarFunction, result: VectorSchemaRoot, rowCount: Int): Unit = {
    require(result.getSchema == call.function.outputSchema,
      s"${call.function.prettyName} returned an unexpected Arrow schema")
    require(result.getRowCount == rowCount, s"${call.function.prettyName} must preserve the input row count")
    result.getFieldVectors.asScala.foreach { vector =>
      require(vector.getValueCount == rowCount, "Output vector has an unexpected row count")
    }
  }
}
