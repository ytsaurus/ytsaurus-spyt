package tech.ytsaurus.spyt.test

import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.{SerializationContext, WriteSerializationContext, WriteTable}
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedRowSerializer, WireRowDeserializer, WireValueDeserializer}
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.core.rows.YTreeRowSerializer
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.RichJavaMap
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.yson.YsonConsumer
import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode, YTreeNodeUtils, YTreeTextSerializer}

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait TestUtils {
  val longColumnSchema: TableSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("value", ColumnValueType.INT64)
    .build()

  private def ytSchema(path: String, fieldName: String)(implicit yt: CompoundClient): java.util.Map[String, YTreeNode] = {
    import scala.collection.JavaConverters._

    val schema = YtWrapper.attribute(path, "schema")
    schema.asList()
      .asScala
      .find((t: YTreeNode) => t.asMap().getOrThrow("name").stringValue() == fieldName)
      .get.asMap()
  }

  def typeV3(path: String, fieldName: String)(implicit yt: CompoundClient): String = {
    ytSchema(path, fieldName)
      .getOrThrow("type_v3").stringValue()
  }

  def typeV1(path: String, fieldName: String)(implicit yt: CompoundClient): String = {
    ytSchema(path, fieldName)
      .getOrThrow("type").stringValue()
  }

  def createEmptyTable(path: String, schema: TableSchema)
                      (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._
    yt.createNode(path, CypressNodeType.TABLE, Map("schema" -> schema.toYTree).asJava).join()
  }

  def readTableAsYson(path: String, transaction: Option[String] = None)
                     (implicit yt: CompoundClient): Seq[YTreeNode] = {
    readTableAsYson(YPath.simple(YtWrapper.formatPath(path)), transaction)
  }

  def readTableAsYson(path: YPath, transaction: Option[String])
                     (implicit yt: CompoundClient): Seq[YTreeNode] = {
    val schema = TableSchema.fromYTree(YtWrapper.attribute(path, "schema", transaction))
    val deser = new WireRowDeserializer[YTreeNode] with WireValueDeserializer[Unit] {
      private var builder = new YTreeBuilder().beginMap()

      override def onNewRow(i: Int): WireValueDeserializer[_] = {
        builder = new YTreeBuilder().beginMap()
        this
      }

      override def onCompleteRow(): YTreeNode = builder.endMap().build()

      override def onNullRow(): YTreeNode = ???

      override def setId(i: Int): Unit = builder.key(schema.getColumnName(i))

      override def setType(columnValueType: ColumnValueType): Unit = {}

      override def setAggregate(b: Boolean): Unit = {}

      override def setTimestamp(l: Long): Unit = {}

      override def build(): Unit = {}

      override def onEntity(): Unit = builder.entity()

      override def onInteger(value: Long): Unit = builder.value(value)

      override def onBoolean(value: Boolean): Unit = builder.value(value)

      override def onDouble(value: Double): Unit = builder.value(value)

      override def onBytes(bytes: Array[Byte]): Unit = builder.value(bytes)
    }
    YtWrapper.readTable(path, deser, 1 minute, transaction, () => _).toList
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: TableSchema,
                         optimizeFor: OptimizeMode = OptimizeMode.Scan,
                         options: Map[String, YTreeNode] = Map.empty)
                        (implicit yt: CompoundClient): Unit = {
    writeTableFromYson(rows, path, schema.toYTree, schema, optimizeFor, options)
  }

  def writeTableFromYson(rows: Seq[String], path: String, schema: YTreeNode, physicalSchema: TableSchema,
                         optimizeFor: OptimizeMode, options: Map[String, YTreeNode])
                        (implicit yt: CompoundClient): Unit = {
    YtWrapper.createTable(path, options ++ Map("schema" -> schema, "optimize_for" -> optimizeFor.node), None, false)
    overwriteTableFromYson(rows, path, physicalSchema)
  }

  def overwriteTableFromYson(rows: Seq[String], path: String, physicalSchema: TableSchema)
                            (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._

    val serializer = new YTreeRowSerializer[String] {
      override def serialize(obj: String, consumer: YsonConsumer): Unit = {
        val node = YTreeTextSerializer.deserialize(new ByteArrayInputStream(obj.getBytes(StandardCharsets.UTF_8)))
        YTreeNodeUtils.walk(node, consumer, false)
      }

      override def getClazz: Class[String] = classOf[String]

      override def deserialize(node: YTreeNode): String = ???

      override def getColumnValueType: TiType = TiType.string()

      override def serializeRow(obj: String, consumer: YsonConsumer, keyFieldsOnly: Boolean, compareWith: String): Unit = {
        serialize(obj, consumer)
      }
    }
    val req = WriteTable.builder[String]()
      .setPath(YPath.simple(path))
      .setSerializationContext(new SerializationContext(serializer))
      .setNeedRetries(false)
      .setTableSchema(physicalSchema)
    val writer = yt.writeTableV2(req.build()).join()
    val rowsJList: java.util.List[String] = rows.asJava

    val chunkRowsLimit = 5000000;

    def write(fromIndex: Int): CompletableFuture[Void] = {
      val toIndex = Math.min(fromIndex + chunkRowsLimit, rowsJList.size())
      val wFuture = writer.write(rowsJList.subList(fromIndex, toIndex))
      wFuture.thenCompose { _ =>
        if (toIndex < rowsJList.size()) {
          write(toIndex)
        } else {
          CompletableFuture.completedFuture[Void](null)
        }
      }
    }

    write(0).thenCompose(_ => writer.finish().asInstanceOf[CompletionStage[Any]]).join()
  }

  def writeTableFromURow(rows: Seq[UnversionedRow], path: String,
                         physicalSchema: TableSchema, optimizeFor: OptimizeMode = OptimizeMode.Scan,
                         options: Map[String, YTreeNode] = Map.empty)
                        (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._

    YtWrapper.createTable(path, options ++ Map("schema" -> physicalSchema.toYTree,
      "optimize_for" -> optimizeFor.node), None, false)

    val req = WriteTable.builder[UnversionedRow]()
      .setPath(path)
      .setSerializationContext(new WriteSerializationContext(new UnversionedRowSerializer(physicalSchema)))
      .setNeedRetries(false)
    val writer = yt.writeTable(req).join()

    @tailrec
    def write(): Unit = {
      if (!writer.write(rows.asJava, physicalSchema)) {
        writer.readyEvent().join()
        write()
      }
    }

    write()
    writer.close().join()
  }

  def writeFileFromStream(input: InputStream, path: String)
                         (implicit yt: CompoundClient): Unit = {
    YtWrapper.createFile(path)
    val out = YtWrapper.writeFile(path, 1 minute, None)
    try {
      val b = new Array[Byte](65536)
      Stream.continually(input.read(b)).takeWhile(_ > 0).foreach(out.write(b, 0, _))
    } finally {
      out.close()
    }
  }

  def writeFileFromString(input: String, path: String)
                         (implicit yt: CompoundClient): Unit = {
    writeFileFromStream(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), path)
  }


  def writeFileFromResource(inputPath: String, path: String)
                           (implicit yt: CompoundClient): Unit = {
    val in = getClass.getResourceAsStream(inputPath)
    try {
      writeFileFromStream(in, path)
    } finally {
      in.close()
    }
  }

  def writeComplexTable(path: String)(implicit yt: CompoundClient): Unit = {
    val ytSchema = TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("f1", ColumnValueType.ANY)
      .addValue("f2", ColumnValueType.ANY)
      .addValue("f3", ColumnValueType.ANY)
      .addValue("f4", ColumnValueType.ANY)
      .addValue("f5", ColumnValueType.ANY)
      .addValue("f6", ColumnValueType.ANY)
      .addValue("f7", ColumnValueType.ANY)
      .addValue("f8", ColumnValueType.ANY)
      .addValue("f9", ColumnValueType.ANY)
      .addValue("f10", ColumnValueType.ANY)
      .addValue("f11", ColumnValueType.ANY)
      .build()
    writeTableFromYson(Seq(
      """{
        |f1={a={aa=1};b=#;c={cc=#}};
        |f2={a={a="aa"};b=#;c={a=#}};
        |f3={a=[0.1];b=#;c=[#]};
        |f4={a=%true;b=#};
        |f5={a={a=1;b=#};b={a="aa"};c=[%true;#];d=0.1};
        |f6=[{a=1;b=#};#];
        |f7=[{a="aa"};{a=#};#];
        |f8=[[1;#];#];
        |f9=[0.1;#];
        |f10=[[1;{a=%true}];[2;{b=%false}];[3;{c=#}];[4;#]];
        |f11=[[{a=%true};1];[{b=%false};2];[{c=#};3];];
        |}""".stripMargin
    ), path, ytSchema)
  }

  /**
   * A utility method than can be used in any test to pause test execution and investigate intermediate state.
   * SPYT_TEST_CONSOLE env variable must be set to true in order to enable this functionality.
   */
  def pressAnyKey(): Unit = {
    if (sys.env.get("SPYT_TEST_CONSOLE").exists(_.toBoolean)) {
      println("Press any key to continue.....")
      scala.io.StdIn.readLine()
    }
  }
}
