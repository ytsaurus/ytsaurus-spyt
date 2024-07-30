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
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Write.DynBatchSize
import tech.ytsaurus.spyt.fs.conf.ConfigEntry
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtOutputCommitter(jobId: String,
                        outputPath: String,
                        dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  private val richPath = YPathEnriched.fromPath(new Path(outputPath))
  private val tmpRichPath = richPath.parent.child(richPath.path.getName + "_tmp")

  @transient private val deletedDirectories = ThreadLocal.withInitial[Seq[Path]](() => Nil)

  import YtOutputCommitter._
  import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration._

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    val externalTransaction = jobContext.getConfiguration.getYtConf(WriteTransaction)

    log.debug(s"Setting up job for path $richPath")

    if (isDynamicTable(conf)) {
      setupDynamicTable(richPath, conf)
    } else {
      withTransaction(createTransaction(conf, GlobalTransaction, externalTransaction)) { transaction =>
        deletedDirectories.get().foreach(p => YtWrapper.remove(YPathEnriched.fromPath(p).toStringYPath, Some(transaction)))
        deletedDirectories.set(Nil)
        if (isTableSorted(conf)) {
          setupSortedTmpTables(transaction)
        }
        if (isTable(conf)) {
          setupTable(richPath, conf, transaction)
        } else {
          setupFiles(transaction)
        }
      }
    }
  }

  private def setupSortedTmpTables(transaction: String)(implicit yt: CompoundClient): Unit = {
    YtWrapper.createDir(tmpRichPath.toYPath, Some(transaction), ignoreExisting = false)
  }

  private def setupFiles(transaction: String)(implicit yt: CompoundClient): Unit = {
    YtWrapper.createDir(richPath.toYPath, Some(transaction), ignoreExisting = false)
  }

  private def setupTable(path: YPathEnriched, conf: Configuration, transaction: String)
                        (implicit yt: CompoundClient): Unit = {
    if (!YtWrapper.exists(path.toStringYPath, Some(transaction))) {
      val options = YtTableSparkSettings.deserialize(conf)
      YtWrapper.createTable(path.toStringYPath, options, Some(transaction))
    }
  }

  private def setupDynamicTable(path: YPathEnriched, conf: Configuration)(implicit yt: CompoundClient): Unit = {
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

  private def setupTmpTable(taskContext: TaskAttemptContext, transaction: String): Unit = {
    val conf = taskContext.getConfiguration
    setupTable(tmpTablePath(taskContext), conf, transaction)(yt(conf))
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    if (!isDynamicTable(conf)) {
      val parent = YtOutputCommitter.getGlobalWriteTransaction(conf)
      withTransaction(createTransaction(conf, Transaction, Some(parent))) { transaction =>
        if (isTableSorted(conf)) setupTmpTable(taskContext, transaction)
      }
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    deletedDirectories.set(Nil)
    val conf = jobContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    if (!isDynamicTable(conf)) {
      abortTransaction(conf, GlobalTransaction)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    if (!isDynamicTable(conf)) {
      abortTransaction(taskContext.getConfiguration, Transaction)
    }
  }

  private def concatenateSortedTables(conf: Configuration, transaction: String): Unit = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
    val sRichPath = richPath.toStringYPath
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

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val conf = jobContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    if (!isDynamicTable(conf)) {
      withTransaction(YtOutputCommitter.getGlobalWriteTransaction(conf)) { transaction =>
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
    if (isDynamicTable(conf)) {
      return FileCommitProtocol.EmptyTaskCommitMessage
    }
    val transactionId = conf.ytConf(Transaction)
    muteTransaction(transactionId)
    new FileCommitProtocol.TaskCommitMessage(transactionId)
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    deletedDirectories.set(path +: deletedDirectories.get())
    true
  }

  private def tmpTablePath(taskContext: TaskAttemptContext): YPathEnriched = {
    tmpRichPath.child(s"part-${taskContext.getTaskAttemptID.getTaskID.getId}")
  }

  private def partFilename(taskContext: TaskAttemptContext, ext: String): String = {
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val conf = taskContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    val path = if (isTableSorted(conf) && !isDynamicTable(conf)) {
      tmpTablePath(taskContext)
    } else if (isTable(conf)) {
      richPath
    } else {
      richPath.child(partFilename(taskContext, ext)).withTransaction(conf.ytConf(Transaction))
    }
    path.toStringPath
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    richPath.toStringPath
  }

  override def onTaskCommit(taskCommit: FileCommitProtocol.TaskCommitMessage): Unit = {
    if (taskCommit == FileCommitProtocol.EmptyTaskCommitMessage) {
      return
    }
    val transactionGuid = taskCommit.obj.asInstanceOf[String]
    val yt = YtClientProvider.cachedClient("committer").yt
    log.debug(s"Commit write transaction: $transactionGuid")
    log.debug(s"Send commit transaction request: $transactionGuid")
    YtWrapper.commitTransaction(transactionGuid)(yt)
    log.debug(s"Success commit transaction: $transactionGuid")
  }
}

object YtOutputCommitter {

  import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration._

  private val log = LoggerFactory.getLogger(getClass)

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

  def createTransaction(conf: Configuration, confEntry: ConfigEntry[String], parent: Option[String]): String = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
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

  def muteTransaction(transaction: String): Unit = {
    pingFutures.get(transaction).foreach { transaction =>
      transaction.stopPing()
    }
  }

  def abortTransaction(conf: Configuration, confEntry: ConfigEntry[String]): Unit = {
    abortTransaction(conf.ytConf(confEntry))
  }

  def abortTransaction(transaction: String): Unit = {
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

  def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(GlobalTransaction)
  }

  def isDynamicTable(conf: Configuration)(implicit yt: CompoundClient): Boolean = {
    conf.getYtConf(YtTableSparkSettings.Path).exists(ytPath => YtWrapper.isDynamicTable(ytPath))
  }
}
