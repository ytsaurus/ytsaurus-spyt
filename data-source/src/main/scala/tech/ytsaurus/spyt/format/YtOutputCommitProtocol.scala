package tech.ytsaurus.spyt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, TaskAttemptID}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.scheduler.{DAGSchedulerUtils, SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.request.{DistributedWriteCookie, StartDistributedWriteSession, TransactionalOptions, WriteFragmentResult}
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient, DistributedWriteHandle}
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.exceptions._
import tech.ytsaurus.spyt.format.YtOutputCommitProtocol._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Write.DynBatchSize
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration.{GlobalTransaction, Transaction}
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{InconsistentDynamicWrite, WriteTransaction, isTable, isTableSorted}
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration, YtTableSparkSettings}
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.config.{ConfigEntry, _}

import java.io.{IOException, ObjectOutputStream}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Semaphore}
import scala.collection.JavaConverters._
import scala.concurrent.Promise

abstract class AbstractYtOutputCommitProtocol(
  jobId: String,
  outputPath: String,
  dynamicPartitionOverwrite: Boolean,
) extends FileCommitProtocol with Serializable {

  protected val rootPath: YPathEnriched = YPathEnriched.fromPath(new Path(outputPath))

  @transient protected lazy implicit val client: CompoundClient = cachedClient

  @transient private val deletedDirectories = ThreadLocal.withInitial[Seq[Path]](() => Nil)

  protected def prepareWrite(conf: Configuration)(transactionActions: String => Unit): Unit = {
    val externalTransaction = conf.getYtConf(WriteTransaction)

    log.debug(s"Setting up job for path $rootPath")
    withTransaction(createTransaction(conf, GlobalTransaction, externalTransaction)) { transaction =>
      deletedDirectories.get()
        .map(YPathEnriched.fromPath(_).toStringYPath)
        .foreach(YtWrapper.remove(_, Some(transaction)))
      deletedDirectories.set(Nil)
      transactionActions(transaction)
    }
  }

  protected def setupTable(path: YPathEnriched, conf: Configuration, transaction: String): Unit = {
    val options = YtTableSparkSettings.deserialize(conf)
    YtWrapper.createTable(path.toStringYPath, options, Some(transaction), ignoreExisting = true)
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    rootPath.toStringPath
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val conf = jobContext.getConfiguration
    doCommitJob(conf, taskCommits)
    commitTransaction(conf, GlobalTransaction)
  }

  protected def doCommitJob(conf: Configuration, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit

  override def abortJob(jobContext: JobContext): Unit = {
    deletedDirectories.set(Nil)
    abortTransactionIfExists(jobContext.getConfiguration, GlobalTransaction)
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    deletedDirectories.set(path +: deletedDirectories.get())
    true
  }
}

class YtOutputCommitProtocol(
  jobId: String,
  outputPath: String,
  dynamicPartitionOverwrite: Boolean
) extends AbstractYtOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  private val tmpRichPath = rootPath.withName(rootPath.name + "_tmp")

  @transient private var writtenTables: ThreadLocal[Seq[YPathEnriched]] = _

  initWrittenTables()

  private def initWrittenTables(): Unit = this.synchronized {
    if (writtenTables == null) writtenTables = ThreadLocal.withInitial(() => Nil)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration

    prepareWrite(conf) { transaction =>
      if (isTable(conf)) {
        if (isTableSorted(conf)) {
          setupTmpTablesDirectory(transaction)
          setupTable(rootPath, conf, transaction)
        }
      } else {
        setupFiles(transaction)
      }
    }
  }

  private def setupTmpTablesDirectory(transaction: String): Unit = {
    YtWrapper.createDir(tmpRichPath.toYPath, Some(transaction), ignoreExisting = false)
  }

  private def setupFiles(transaction: String): Unit = {
    YtWrapper.createDir(rootPath.toYPath, Some(transaction), ignoreExisting = false)
  }

  private def setupSortedTablePart(taskContext: TaskAttemptContext): YPathEnriched = {
    val tmpPath = tmpRichPath.child(s"part-${taskContext.getTaskAttemptID.getTaskID.getId}")
    withTransaction(getWriteTransaction(taskContext.getConfiguration)) { transaction =>
      setupTable(tmpPath, taskContext.getConfiguration, transaction)
    }
    tmpPath
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    initWrittenTables()  // Executors will have null value after deserialization
    val parent = YtOutputCommitProtocol.getGlobalWriteTransaction(conf)
    createTransaction(
      conf, Transaction, Some(parent),
      title = Some(s"YT Operation ID ${System.getenv("YT_OPERATION_ID")}, YT Job ID ${System.getenv("YT_JOB_ID")}, Spark ${taskContext.getTaskAttemptID}"),
    )
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    abortTransactionIfExists(taskContext.getConfiguration, Transaction)
  }

  private def concatenateSortedTables(conf: Configuration, transaction: String): Unit = {
    val sRichPath = rootPath.toStringYPath
    val sTmpRichPath = tmpRichPath.toStringYPath
    val tmpTables = YtWrapper.listDir(sTmpRichPath, Some(transaction)).map(tmpRichPath.child).map(_.toStringYPath)
    try {
      YtWrapper.concatenate(sRichPath +: tmpTables, sRichPath, Some(transaction))
    } catch {
      case e: RuntimeException =>
        logWarning("Concatenate operation failed. Fallback to merge", e)
        YtWrapper.mergeTables(sTmpRichPath, sRichPath, sorted = true,
          Some(transaction), conf.getYtSpecConf("merge"))
    }
    YtWrapper.remove(sTmpRichPath, Some(transaction))
  }

  private def renameTmpPartitionTables(taskMessages: Seq[TaskMessage], transaction: Option[String]): Unit = {
    val tables = taskMessages.flatMap(_.tables).filter(_.name.startsWith(tmpPartitionPrefix)).distinct
    tables.foreach { path =>
      val targetPath = path.withName(path.name.drop(tmpPartitionPrefix.length))
      YtWrapper.move(path.toStringYPath, targetPath.toStringYPath, transaction, force = true)
    }
  }

  override def doCommitJob(conf: Configuration, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    withTransaction(YtOutputCommitProtocol.getGlobalWriteTransaction(conf)) { transaction =>
      renameTmpPartitionTables(taskCommits.map(_.obj.asInstanceOf[TaskMessage]), Some(transaction))
      if (isTableSorted(conf)) {
        concatenateSortedTables(conf, transaction)
      }
    }
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    val conf = taskContext.getConfiguration
    val optTaskTransactionId = conf.getYtConf(Transaction)
    if (optTaskTransactionId.isDefined) {
      muteTransaction(optTaskTransactionId.get)
    }
    new FileCommitProtocol.TaskCommitMessage(TaskMessage(writtenTables.get(), optTaskTransactionId))
  }

  private def partFilename(taskContext: TaskAttemptContext, ext: String): String = {
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val conf = taskContext.getConfiguration
    val fullPath = dir.map(rootPath.child).getOrElse(rootPath)
    val path = if (isTable(conf)) {
      if (isTableSorted(conf)) {
        if (dir.isDefined) hivePartitioningNotSupportedError("sorted static tables")
        setupSortedTablePart(taskContext)
      } else {
        // In case of dynamic partition we will write to another table and then rename with overwrite
        val p = if (dynamicPartitionOverwrite) fullPath.withName(tmpPartitionPrefix + fullPath.name) else fullPath
        setupTable(p, conf, YtOutputCommitProtocol.getGlobalWriteTransaction(conf))
        p
      }
    } else {
      fullPath.child(partFilename(taskContext, ext)).withTransaction(conf.ytConf(Transaction))
    }
    writtenTables.set(path +: writtenTables.get())
    path.toStringPath
  }

  override def onTaskCommit(taskCommit: FileCommitProtocol.TaskCommitMessage): Unit = {
    val taskMessage = taskCommit.obj.asInstanceOf[TaskMessage]
    if (taskMessage.transactionId.isEmpty) {
      return
    }
    val transactionGuid = taskMessage.transactionId.get
    log.debug(s"Commit write transaction: $transactionGuid")
    log.debug(s"Send commit transaction request: $transactionGuid")
    YtWrapper.commitTransaction(transactionGuid)
    log.debug(s"Success commit transaction: $transactionGuid")
  }
}

class DynamicTableOutputCommitProtocol(
  jobId: String,
  outputPath: String,
  dynamicPartitionOverwrite: Boolean
) extends AbstractYtOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    validateDynamicTable(rootPath, conf)
  }

  private def validateDynamicTable(path: YPathEnriched, conf: Configuration): Unit = {
    if (!YtWrapper.isMounted(path.toStringYPath)) {
      throw TableNotMountedException("Dynamic table should be mounted before writing to it")
    }

    val inconsistentDynamicWrite = conf.ytConf(InconsistentDynamicWrite)
    if (!inconsistentDynamicWrite) {
      throw InconsistentDynamicWriteException("For dynamic tables you should explicitly specify an additional " +
        "option inconsistent_dynamic_write with true value so that you do agree that there is no support (yet) for " +
        "transactional writes to dynamic tables")
    }

    val maxDynBatchSize = DynBatchSize.default.get
    val dynBatchSize = conf.get(s"spark.yt.${DynBatchSize.name}", maxDynBatchSize.toString).toInt
    if (dynBatchSize > maxDynBatchSize) {
      throw TooLargeBatchException(s"spark.yt.${DynBatchSize.name} must be set to no more than $maxDynBatchSize for dynamic tables")
    }
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = ()

  override def doCommitJob(conf: Configuration, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = ()

  override def abortJob(jobContext: JobContext): Unit = ()

  override def setupTask(taskContext: TaskAttemptContext): Unit = ()

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    if (dir.isDefined) {
      hivePartitioningNotSupportedError("dynamic tables")
    }
    rootPath.toStringPath
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    FileCommitProtocol.EmptyTaskCommitMessage
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = ()
}

class DistributedWriteOutputCommitProtocol(
  jobId: String,
  outputPath: String,
  dynamicPartitionOverwrite: Boolean
) extends AbstractYtOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  @transient @volatile private var dwHandleOpt: Option[DistributedWriteHandle] = None

  @volatile private var cookiesBroadcast: Broadcast[java.util.List[DistributedWriteCookie]] = _
  @transient private val cookiesSemaphore = new Semaphore(0)

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    // We need to override the default serialization behaviour here because cookiesBroadcast is set in a separate thread
    // and we need to wait for it
    if (Thread.currentThread().getName == "dag-scheduler-event-loop") {
      // We only need to wait for a cookiesBroadcast to be set when an event occurs in dag-scheduler-event-loop, but
      // not inside other threads
      cookiesSemaphore.acquire()
    }
    out.defaultWriteObject()
  }

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    prepareWrite(conf) { transaction =>
      setupTable(rootPath, conf, transaction)
    }

    val transactionId = getGlobalWriteTransaction(conf)

    val sc = SparkSession.active.sparkContext
    sc.setLocalProperty(distributedWritePropKey, jobId)

    // We need to use SparkListener here to defer the start of distributed write process because at this moment we
    // don't know the number of spark tasks on the resulting stage
    val sparkJobListener: SparkListener = new SparkListener() {
      private var sparkJobId: Int = -1

      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        // There might be more than one job that can be submitted at the same time from different threads, so it is
        // needed to check every jobStart event to match to correct jobId
        if (jobStart.properties.get(distributedWritePropKey) == jobId) {
          val numOutputTasksOpt = DAGSchedulerUtils.getNumOutputTasks(sc, jobStart.stageIds)
          if (numOutputTasksOpt.isDefined && !jobStart.properties.containsKey("spark.rdd.scope")) {
            sparkJobId = jobStart.jobId
            val cookieCount = numOutputTasksOpt.get
            val distributedWriteRequest = StartDistributedWriteSession.builder()
              .setPath(rootPath.toYPath)
              .setCookieCount(cookieCount)
              .setTransactionalOptions(new TransactionalOptions(GUID.valueOf(transactionId)))
              .build()
            val dwHandle = client.startDistributedWriteSession(distributedWriteRequest).join()
            dwHandleOpt = Some(dwHandle)
          }
        }
      }

      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        if (DAGSchedulerUtils.isResultStageForJobId(sc, sparkJobId, stageSubmitted.stageInfo.stageId)) {
          stopCookiesBroadcast()
          cookiesBroadcast = sc.broadcast(dwHandleOpt.get.getCookies)
          cookiesSemaphore.release()
        }
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        if (sparkJobId == jobEnd.jobId) {
          sc.removeSparkListener(this)
        }
      }
    }
    sc.addSparkListener(sparkJobListener)
  }

  override def doCommitJob(conf: Configuration, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    stopCookiesBroadcast()
    val results = taskCommits.map(_.obj.asInstanceOf[WriteFragmentResult]).filter(_ != null)
    dwHandleOpt.foreach(_.finish(results.asJava).join())
  }

  override def abortJob(jobContext: JobContext): Unit = {
    stopCookiesBroadcast()
    dwHandleOpt.foreach(_.stopPing())
    super.abortJob(jobContext)
  }

  private def stopCookiesBroadcast(): Unit = {
    if (cookiesBroadcast != null) {
      cookiesBroadcast.destroy()
      cookiesBroadcast = null
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val partitionId = TaskContext.get().partitionId()
    val cookie = cookiesBroadcast.value.get(partitionId)
    setCookieForTask(taskContext, cookie)
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    if (dir.isDefined) {
      hivePartitioningNotSupportedError("distributed write")
    }
    rootPath.toStringPath
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    deleteCookieForTask(taskContext)
    val result = getAndRemoveWriteFragmentResult()
    new FileCommitProtocol.TaskCommitMessage(result)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    deleteCookieForTask(taskContext)
  }
}

object YtOutputCommitProtocol {
  import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration._

  // These messages will be sent from successful tasks to a driver
  private case class TaskMessage(tables: Seq[YPathEnriched], transactionId: Option[String])

  private val log = LoggerFactory.getLogger(getClass)

  private val tmpPartitionPrefix = ".tmp-part_"

  private val pingFutures = scala.collection.concurrent.TrieMap.empty[String, ApiServiceTransaction]

  val distributedWritePropKey = "distributedWriteJobId"

  private val taskCookies: ConcurrentMap[TaskAttemptID, DistributedWriteCookie] = new ConcurrentHashMap()

  def setCookieForTask(taskContext: TaskAttemptContext, cookie: DistributedWriteCookie): Unit = {
    taskCookies.put(taskContext.getTaskAttemptID, cookie)
  }

  def getCookieForTask(taskContext: TaskAttemptContext): DistributedWriteCookie = {
    taskCookies.get(taskContext.getTaskAttemptID)
  }

  def deleteCookieForTask(taskContext: TaskAttemptContext): Unit = {
    taskCookies.remove(taskContext.getTaskAttemptID)
  }

  private val writeFragmentResults: ThreadLocal[WriteFragmentResult] = new ThreadLocal[WriteFragmentResult]()

  def setWriteFragmentResult(result: WriteFragmentResult): Unit = {
    writeFragmentResults.set(result)
  }

  def getAndRemoveWriteFragmentResult(): WriteFragmentResult = {
    val result = writeFragmentResults.get()
    writeFragmentResults.remove()
    result
  }

  lazy val cachedClient: CompoundClient = {
    val conf = SparkEnv.get.conf
    YtClientProvider.ytClient(ytClientConfiguration(conf))
  }

  def withTransaction(transaction: String)(f: String => Unit): Unit = {
    try {
      f(transaction)
    } catch {
      case e: Throwable =>
        try {
          abortTransaction(transaction)
        } catch {
          case inner: Throwable =>
            e.addSuppressed(inner)
        }
        throw e
    }
  }

  def createTransaction(conf: Configuration, confEntry: ConfigEntry[String], parent: Option[String], title: Option[String] = None)
                       (implicit yt: CompoundClient): String = {
    val transactionTimeout = conf.ytConf(SparkYtConfiguration.Transaction.Timeout)
    val pingInterval = conf.ytConf(SparkYtConfiguration.Transaction.PingInterval)
    val transaction = YtWrapper.createTransaction(parent, transactionTimeout, title = title, pingPeriod = pingInterval)
    try {
      pingFutures += transaction.getId.toString -> transaction
      log.debug(s"Create write transaction: ${transaction.getId}")
      conf.setYtConf(confEntry, transaction.getId.toString)
      transaction.getId.toString
    } catch {
      case e: Throwable =>
        abortTransaction(transaction.getId.toString)
        throw e
    }
  }

  def abortTransactionIfExists(conf: Configuration, confEntry: ConfigEntry[String]): Unit = {
    val transaction = conf.getYtConf(confEntry)
    transaction.foreach(abortTransaction)
  }

  private def abortTransaction(transaction: String): Unit = {
    log.debug(s"Abort write transaction: $transaction")
    pingFutures.remove(transaction).foreach { transaction =>
      transaction.abort().join()
    }
  }

  private def muteTransaction(transactionGuid: String): Unit = {
    pingFutures.remove(transactionGuid).foreach { transaction =>
      transaction.stopPing()
    }
  }

  def commitTransaction(conf: Configuration, confEntry: ConfigEntry[String]): Unit = {
    withTransaction(conf.ytConf(confEntry)) { transactionGuid =>
      log.debug(s"Commit write transaction: $transactionGuid")
      pingFutures.remove(transactionGuid).foreach { transaction =>
        log.debug(s"Send commit transaction request: $transactionGuid")
        transaction.commit().join()
        log.debug(s"Successfully committed transaction: $transactionGuid")
      }
    }
  }

  def getWriteTransaction(conf: Configuration): String = {
    conf.ytConf(Transaction)
  }

  def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(GlobalTransaction)
  }

  def hivePartitioningNotSupportedError(description: String): Unit = {
    throw new IllegalStateException(s"Hive partitioning is not supported for $description")
  }
}
