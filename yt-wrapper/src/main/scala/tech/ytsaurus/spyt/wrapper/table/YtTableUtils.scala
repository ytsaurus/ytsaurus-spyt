package tech.ytsaurus.spyt.wrapper.table

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.Utils
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.RichJavaMap
import tech.ytsaurus.spyt.wrapper.cypress.YtCypressUtils
import tech.ytsaurus.spyt.wrapper.operation.OperationStatus
import tech.ytsaurus.spyt.wrapper.transaction.YtTransactionUtils
import tech.ytsaurus.client.{AsyncReader, CompoundClient, TablePartitionRowsetReader}
import tech.ytsaurus.client.request.{TablePartitionCookie, _}
import tech.ytsaurus.client.rows.WireRowDeserializer
import tech.ytsaurus.core.{DataSize, GUID}
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.rpcproxy.{EOperationType, ERowsetFormat}
import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode, YTreeTextSerializer}

import java.nio.ByteBuffer
import java.nio.file.Paths
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait YtTableUtils {
  self: YtCypressUtils with YtTransactionUtils =>

  private val log = LoggerFactory.getLogger(getClass)

  def createTable(path: String,
                  settings: YtTableSettings,
                  transaction: Option[String] = None,
                  ignoreExisting: Boolean = false)
                 (implicit yt: CompoundClient): Unit = {
    val parent = Paths.get(path).getParent
    if (parent != null) {
      createDir(parent.toString, transaction, ignoreExisting = true)
    }
    createTable(path, settings.options, transaction, ignoreExisting = ignoreExisting)
  }

  def createTable(path: String,
                  settings: YtTableSettings)
                 (implicit yt: CompoundClient): Unit = {
    createTable(path, settings, None)
  }

  def createTable(path: String,
                  options: Map[String, YTreeNode],
                  transaction: Option[String],
                  ignoreExisting: Boolean)
                 (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create table: $path, transaction: $transaction")
    import scala.collection.JavaConverters._
    val request = CreateNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setType(CypressNodeType.TABLE)
      .setAttributes(options.asJava)
      .setIgnoreExisting(ignoreExisting)
      .optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  def readTable[T](path: YPath, deserializer: WireRowDeserializer[T], timeout: Duration = 1 minute,
                   transaction: Option[String] = None, reportBytesRead: Long => Unit = _ => ())
                  (implicit yt: CompoundClient): SyncTableIterator[T] = {
    val request = ReadTable.builder()
      .setPath(path)
      .setSerializationContext(new ReadSerializationContext(deserializer))
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
      .optionalTransaction(transaction)
    val reader = yt.readTable(request).join()
    new SyncTableIterator(reader, timeout, reportBytesRead)
  }

  def readTableArrowStream(path: String, timeout: Duration, transaction: Option[String],
                           reportBytesRead: Long => Unit)
                          (implicit yt: CompoundClient): TableCopyByteStream = {
    val request = ReadTable.builder[ByteBuffer]().setPath(path)
      .setSerializationContext(ReadSerializationContext.binaryArrow())
      .optionalTransaction(transaction)
    val reader = yt.readTable(request).join()
    new TableCopyByteStream(reader, reportBytesRead)
  }

  def readTableArrowStream(path: YPath, timeout: Duration = 1 minute,
                           transaction: Option[String] = None,
                           reportBytesRead: Long => Unit = _ => {})
                          (implicit yt: CompoundClient): TableCopyByteStream = {
    readTableArrowStream(path.toString, timeout, transaction, reportBytesRead)
  }

  private def startedBy(builder: YTreeBuilder): YTreeBuilder = {
    import tech.ytsaurus.spyt.BuildInfo
    builder
      .beginMap
      .key("user").value(System.getProperty("user.name"))
      .key("command").beginList.value("command").endList
      .key("hostname").value(Utils.ytHostnameOrIpAddress)
      .key("pid").value(ProcessHandle.current().pid())
      .key("wrapper_version").value(BuildInfo.ytClientVersion)
      .endMap
  }

  private def pathToTree(path: String, transaction: Option[String]): YTreeNode = {
    transaction.foldLeft(YPath.simple(formatPath(path))) { case (p, t) =>
      p.plusAdditionalAttribute("transaction_id", t)
    }.toTree
  }

  private def ysonPaths(paths: Seq[String], transaction: Option[String]): YTreeNode = {
    paths.foldLeft(new YTreeBuilder().beginList()) { case (b, path) =>
      b.value(pathToTree(path, transaction))
    }.endList().build()
  }

  private def operationResult(guid: GUID)(implicit yt: CompoundClient): String = {
    val info = yt.getOperation(new GetOperation(guid)).join()
    Option(info.asMap().get("result"))
      .map(YTreeTextSerializer.serialize).getOrElse("unknown")
  }

  @tailrec
  private def awaitOperation(guid: GUID)(implicit yt: CompoundClient): OperationStatus = {
    val info = yt.getOperation(new GetOperation(guid)).join()
    val status = OperationStatus.getByName(info.asMap().getOrThrow("state").stringValue())
    if (status.isFinished) {
      status
    } else {
      log.info(s"Operation $guid is in status $status")
      Thread.sleep((5 seconds).toMillis)
      awaitOperation(guid)
    }
  }

  def mergeTables(srcDir: String, dstTable: String,
                  sorted: Boolean,
                  transaction: Option[String] = None,
                  specParams: Map[String, YTreeNode] = Map.empty)
                 (implicit yt: CompoundClient): Unit = {
    log.debug(s"Merge tables: $srcDir -> $dstTable, transaction: $transaction")

    val srcList = dstTable +: listDir(srcDir, transaction).map(name => s"$srcDir/$name")

    val operationSpec = new YTreeBuilder()
      .beginMap()
      .key("mode").value(if (sorted) "sorted" else "unordered")
      .key("combine_chunks").value(false)
      .key("started_by").apply(startedBy)
      .key("input_table_paths").value(ysonPaths(srcList, None))
      .key("output_table_path").value(pathToTree(dstTable, None))
      .apply(specParams.foldLeft(_) { case (b, (k, v)) => b.key(k).value(v) })
      .endMap()
      .build()

    val operationRequest = new StartOperation(EOperationType.OT_MERGE, operationSpec).toBuilder
      .optionalTransaction(transaction)
    val guid = yt.startOperation(operationRequest).join()

    val finalStatus = awaitOperation(guid)

    if (!finalStatus.isSuccess) {
      throw new IllegalStateException(s"Merge operation finished with unsuccessful status $finalStatus, " +
        s"result is ${operationResult(guid)}")
    }
  }

  def partitionTables(path: YPath, splitBytes: Long, enableCookies: Boolean = false)
                     (implicit yt: CompoundClient): Seq[MultiTablePartition] = {
    import scala.collection.JavaConverters._

    val request = PartitionTables.builder()
      .setPaths(java.util.List.of[YPath](path))
      .setPartitionMode(PartitionTablesMode.Ordered)
      .setDataWeightPerPartition(DataSize.fromBytes(splitBytes))
      .setEnableCookies(enableCookies)
      .build()

    val result = yt.partitionTables(request).join()
    result.asScala.toList
  }

  def createTablePartitionReader[T](cookie: TablePartitionCookie, deserializer: WireRowDeserializer[T],
                                    reportBytesRead: Long => Unit = _ => {})
                                   (implicit yt: CompoundClient): AsyncTableIterator[T] = {
    val context = new ReadSerializationContext(new TablePartitionRowsetReader[T](deserializer))
    val request = CreateTablePartitionReader.builder()
      .setCookie(cookie)
      .setSerializationContext(context)
      .setUnordered(false)
      .setOmitInaccessibleColumns(true)
      .build()
    val reader: AsyncReader[T] = yt.createTablePartitionReader(request).join()
    new AsyncTableIterator(reader, reportBytesRead)
  }

  def createTablePartitionArrowStream(cookie: TablePartitionCookie, reportBytesRead: Long => Unit = _ => {})
                                        (implicit yt: CompoundClient): PartitionCopyByteStream = {
    val request = CreateTablePartitionReader.binaryArrowBuilder()
      .setCookie(cookie)
      .setUnordered(false)
      .setOmitInaccessibleColumns(true)
      .build()
    val reader: AsyncReader[ByteBuffer] = yt.createTablePartitionReader(request).join()
    new PartitionCopyByteStream(reader, reportBytesRead)
  }
}

