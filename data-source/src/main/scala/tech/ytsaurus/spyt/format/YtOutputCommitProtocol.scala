package tech.ytsaurus.spyt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings._
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration, YtTableSparkSettings}
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient}
import tech.ytsaurus.spyt.exceptions._
import tech.ytsaurus.spyt.format.YtOutputCommitProtocol._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Write.DynBatchSize
import tech.ytsaurus.spyt.fs.conf.ConfigEntry
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtOutputCommitProtocol(jobId: String,
                             outputPath: String,
                             dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {

  private val rootPath = YPathEnriched.fromPath(new Path(outputPath))
  private val tmpRichPath = rootPath.withName(rootPath.name + "_tmp")

  @transient private val deletedDirectories = ThreadLocal.withInitial[Seq[Path]](() => Nil)
  @transient private var writtenTables: ThreadLocal[Seq[YPathEnriched]] = _

  initWrittenTables()

  import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration._

  private def initWrittenTables(): Unit = this.synchronized {
    if (writtenTables == null) writtenTables = ThreadLocal.withInitial(() => Nil)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    val externalTransaction = jobContext.getConfiguration.getYtConf(WriteTransaction)

    log.debug(s"Setting up job for path $rootPath")

    if (isDynamicTable(conf)) {
      validateDynamicTable(rootPath, conf)
    } else {
      withTransaction(createTransaction(conf, GlobalTransaction, externalTransaction)) { transaction =>
        deletedDirectories.get().map(YPathEnriched.fromPath(_).toStringYPath).foreach(YtWrapper.remove(_, Some(transaction)))
        deletedDirectories.set(Nil)
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
  }

  private def setupTmpTablesDirectory(transaction: String)(implicit yt: CompoundClient): Unit = {
    YtWrapper.createDir(tmpRichPath.toYPath, Some(transaction), ignoreExisting = false)
  }

  private def setupFiles(transaction: String)(implicit yt: CompoundClient): Unit = {
    YtWrapper.createDir(rootPath.toYPath, Some(transaction), ignoreExisting = false)
  }

  private def setupTable(path: YPathEnriched, conf: Configuration, transaction: String)
                        (implicit yt: CompoundClient): Unit = {
    val options = YtTableSparkSettings.deserialize(conf)
    YtWrapper.createTable(path.toStringYPath, options, Some(transaction), ignoreExisting = true)
  }

  private def validateDynamicTable(path: YPathEnriched, conf: Configuration)(implicit yt: CompoundClient): Unit = {
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
      throw TooLargeBatchException(s"spark.yt.write.batchSize must be set to no more than $maxDynBatchSize for dynamic tables")
    }
  }

  private def setupSortedTablePart(taskContext: TaskAttemptContext)(implicit yt: CompoundClient): YPathEnriched = {
    val tmpPath = tmpRichPath.child(s"part-${taskContext.getTaskAttemptID.getTaskID.getId}")
    withTransaction(getWriteTransaction(taskContext.getConfiguration)) { transaction =>
      setupTable(tmpPath, taskContext.getConfiguration, transaction)
    }
    tmpPath
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    initWrittenTables()  // Executors will have null value after deserialization
    if (!isDynamicTable(conf)) {
      val parent = YtOutputCommitProtocol.getGlobalWriteTransaction(conf)
      createTransaction(conf, Transaction, Some(parent))
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    deletedDirectories.set(Nil)
    abortTransactionIfExists(jobContext.getConfiguration, GlobalTransaction)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    abortTransactionIfExists(taskContext.getConfiguration, Transaction)
  }

  private def concatenateSortedTables(conf: Configuration, transaction: String)(implicit yt: CompoundClient): Unit = {
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

  private def renameTmpPartitionTables(taskMessages: Seq[TaskMessage], transaction: Option[String])
                                      (implicit yt: CompoundClient): Unit = {
    val tables = taskMessages.flatMap(_.tables).filter(_.name.startsWith(tmpPartitionPrefix)).distinct
    tables.foreach { path =>
      val targetPath = path.withName(path.name.drop(tmpPartitionPrefix.length))
      YtWrapper.move(path.toStringYPath, targetPath.toStringYPath, transaction, force = true)
    }
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val conf = jobContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    if (!isDynamicTable(conf)) {
      withTransaction(YtOutputCommitProtocol.getGlobalWriteTransaction(conf)) { transaction =>
        renameTmpPartitionTables(taskCommits.map(_.obj.asInstanceOf[TaskMessage]), Some(transaction))
        if (isTableSorted(conf)) {
          concatenateSortedTables(conf, transaction)
        }
        commitTransaction(conf, GlobalTransaction)
      }
    }
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    val conf = taskContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    if (!isDynamicTable(conf)) commitTransaction(conf, Transaction)
    new FileCommitProtocol.TaskCommitMessage(TaskMessage(writtenTables.get()))
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    deletedDirectories.set(path +: deletedDirectories.get())
    true
  }

  private def partFilename(taskContext: TaskAttemptContext, ext: String): String = {
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  private def hivePartitioningNotSupportedError(description: String): Unit = {
    throw new IllegalStateException(s"Hive partitioning is not supported for $description")
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val conf = taskContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    val fullPath = dir.map(rootPath.child).getOrElse(rootPath)
    val path = if (isTable(conf)) {
      if (isDynamicTable(conf)) {
        if (dir.isDefined) hivePartitioningNotSupportedError("dynamic tables")
        rootPath
      } else if (isTableSorted(conf)) {
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

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    rootPath.toStringPath
  }
}

object YtOutputCommitProtocol {
  import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration._

  // These messages will be sent from successful tasks to a driver
  case class TaskMessage(tables: Seq[YPathEnriched]) extends Serializable

  private val log = LoggerFactory.getLogger(getClass)

  private val tmpPartitionPrefix = ".tmp-part_"

  private val pingFutures = scala.collection.concurrent.TrieMap.empty[String, ApiServiceTransaction]

  private def yt(conf: Configuration): CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf), "committer")

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

  def createTransaction(conf: Configuration, confEntry: ConfigEntry[String], parent: Option[String])
                       (implicit yt: CompoundClient): String = {
    val transactionTimeout = conf.ytConf(SparkYtConfiguration.Transaction.Timeout)
    val transaction = YtWrapper.createTransaction(parent, transactionTimeout)
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

  private def abortTransactionIfExists(conf: Configuration, confEntry: ConfigEntry[String]): Unit = {
    val transaction = conf.getYtConf(confEntry)
    transaction.foreach(abortTransaction)
  }

  private def abortTransaction(transaction: String): Unit = {
    log.debug(s"Abort write transaction: $transaction")
    pingFutures.remove(transaction).foreach { transaction =>
      transaction.abort().join()
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

  private def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(GlobalTransaction)
  }

  def isDynamicTable(conf: Configuration)(implicit yt: CompoundClient): Boolean = {
    conf.getYtConf(YtTableSparkSettings.Path).exists(YtWrapper.isDynamicTable)
  }
}
