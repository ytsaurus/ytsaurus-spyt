package tech.ytsaurus.spyt.test

import tech.ytsaurus.core.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.utils.CollectionUtils
import tech.ytsaurus.spyt.wrapper.YtWrapper.{createTable, insertRows, mountTableSync, reshardTable, unmountTableSync}
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.ysontree.YTreeNode

import java.time.Duration
import java.util.stream.Collectors
import java.util.{List => JList, Map => JMap}

trait DynTableTestUtils {
  self: LocalYtClient =>

  val testSchema: TableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .add(new ColumnSchema("a", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("b", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .addValue("c", ColumnValueType.STRING)
    .build()
  val testSchemaUnsigned: TableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .add(new ColumnSchema("a", ColumnValueType.UINT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("b", ColumnValueType.UINT64, ColumnSortOrder.ASCENDING))
    .addValue("c", ColumnValueType.STRING)
    .build()
  val orderedTestSchema: TableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.INT64)
    .addValue("c", ColumnValueType.STRING)
    .build()

  val testData: Seq[TestRow] = getTestData()
  val testRow: TestRow = TestRow(100, 100, "new_row")

  def getTestData(low: Int = 1, high: Int = 10): Seq[TestRow] = {
    (low to high).map(i => TestRow(i, i * 2, ('A'.toInt + i).toChar.toString))
  }

  def getTestSchema(sorted: Boolean = true): TableSchema = if (sorted) testSchema else orderedTestSchema

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[Seq[Any]] = Nil,
                       schema: TableSchema = testSchema, enableDynamicStoreRead: Boolean = false): Unit = {
    val sortColumns = TestTableSettings.getKeyColumns(schema)
    val options = JMap.of("enable_dynamic_store_read", enableDynamicStoreRead.toString.asInstanceOf[Any])
    createTable(path, TestTableSettings(schema.toYTree, isDynamic = true, sortColumns = sortColumns, options))
    mountTableSync(path, Duration.ofSeconds(10))
    insertRows(path, schema, data.map(r => r.productIterator.toList))
    unmountTableSync(path, Duration.ofSeconds(10))
    if (pivotKeys.nonEmpty) reshardTable(path, schema, pivotKeys)
    mountTableSync(path, Duration.ofSeconds(10))
  }

  def prepareOrderedTestTable(path: String, schema: TableSchema = orderedTestSchema,
                              enableDynamicStoreRead: Boolean = false, tabletCount: Int = 3): Unit = {
    val options = JMap.of("enable_dynamic_store_read", enableDynamicStoreRead.toString, "tablet_count", tabletCount)
    createTable(path, TestTableSettings(schema.toYTree, isDynamic = true, sortColumns = JList.of(), options))
    mountTableSync(path)
  }

  def appendChunksToTestTable(path: String, data: Seq[Seq[TestRow]], sorted: Boolean = true,
                              remount: Boolean = true): Unit = {
    val schema = getTestSchema(sorted)
    data.foreach(chunk => insertRows(path, schema, chunk.map(r => r.productIterator.toList)))
    if (remount) {
      unmountTableSync(path)
      mountTableSync(path)
    }
  }

  def appendChunksToTestTable[T <: Product](path: String, schema: TableSchema, data: Seq[Seq[T]], sorted: Boolean,
                              remount: Boolean): Unit = {
    data.foreach(chunk => insertRows(path, schema, chunk.map(r => r.productIterator.toList)))
    if (remount) {
      unmountTableSync(path)
      mountTableSync(path)
    }
  }
}

case class TestTableSettings(
  ytSchema: YTreeNode,
  isDynamic: Boolean = false,
  sortColumns: JList[String] = JList.of(),
  otherOptions: JMap[String, Any] = JMap.of()) extends YtTableSettings {
  override def optionsAny: JMap[String, Any] = CollectionUtils.concatMaps(otherOptions, JMap.of("dynamic", isDynamic))
}

object TestTableSettings {
  def apply(schema: TableSchema, isDynamic: Boolean): YtTableSettings = {
    TestTableSettings(schema.toYTree, isDynamic, sortColumns = getKeyColumns(schema))
  }

  def getKeyColumns(schema: TableSchema): JList[String] = {
    val keyColumnsStream = schema.getColumns.stream().filter(_.getSortOrder != null).map[String](_.getName)
    keyColumnsStream.collect(Collectors.toList())
  }
}

case class TestRow(a: Long, b: Long, c: String)