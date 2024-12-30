package tech.ytsaurus.spyt.format.batch

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.slf4j.LoggerFactory
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.wrapper.LogLazy
import tech.ytsaurus.spyt.serialization.IndexedDataType
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.YtArrowInputStream

import scala.collection.JavaConverters._

class ArrowBatchReader(stream: YtArrowInputStream, schema: StructType,
                       ytSchema: TableSchema) extends BatchReaderBase with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)

  private val indexedSchema = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))

  private var _allocator: BufferAllocator = _
  private var _reader: ArrowStreamReader = _
  private var _root: VectorSchemaRoot = _
  private var _dictionaries: java.util.Map[java.lang.Long, Dictionary] = _
  private var _columnVectors: Array[ColumnVector] = _
  private var emptySchema = false

  initialize()

  private def initialize(): Unit = {
    if (stream.isEmptyPage) {
      emptySchema = true
    } else {
      updateReader()
      updateBatch()
    }
  }

  override protected def nextBatchInternal: Boolean = {
    if (emptySchema) {
      false
    } else {
      if (stream.isNextPage) updateReader()
      val batchLoaded = _reader.loadNextBatch()
      if (batchLoaded) {
        updateBatch()
        setNumRows(_root.getRowCount)
        true
      } else {
        closeReader()
        false
      }
    }
  }

  private def closeReader(): Unit = {
    Option(_reader).foreach(_.close(false))
    Option(_allocator).foreach(_.close())
  }

  override protected def finalRead(): Unit = {
    val bytes = new Array[Byte](9)
    val res = stream.read(bytes)
    val isAllowedBytes = bytes.forall(_ == 0) || (bytes.take(4).forall(_ == -1) && bytes.drop(4).forall(_ == 0))
    if (res > 8 || !isAllowedBytes) {
      throw new IllegalStateException(s"Final read failed." +
        s" Bytes read: $res; byte buffer: ${bytes.mkString("[", ", ", "]")}")
    }
  }

  override def close(): Unit = {
    stream.close()
  }

  private def updateReader(): Unit = {
    log.debugLazy(s"Update arrow reader, " +
      s"allocated ${Option(_allocator).map(_.getAllocatedMemory)}, " +
      s"peak allocated ${Option(_allocator).map(_.getPeakMemoryAllocation)}")
    closeReader()

    _allocator = new RootAllocator().newChildAllocator(s"arrow reader", 0, Long.MaxValue)
    _reader = new ArrowStreamReader(stream, _allocator)
    _root = _reader.getVectorSchemaRoot
    _dictionaries = _reader.getDictionaryVectors
  }

  private def createArrowColumnVector(vector: FieldVector, dataType: IndexedDataType,
                                      columnType: ColumnValueType): ArrowColumnVector = {
    val dict = Option(vector.getField.getDictionary).flatMap { encoding =>
      if (_dictionaries.containsKey(encoding.getId)) {
        Some(_dictionaries.get(encoding.getId))
      } else None
    }
    new ArrowColumnVector(dataType, vector, dict, columnType)
  }

  private def updateBatch(): Unit = {
    log.traceLazy(s"Read arrow batch, " +
      s"allocated ${Option(_allocator).map(_.getAllocatedMemory)}, " +
      s"peak allocated ${Option(_allocator).map(_.getPeakMemoryAllocation)}")

    _columnVectors = new Array[ColumnVector](schema.fields.length)

    val arrowSchema = _root.getSchema.getFields.asScala.map(_.getName)
    val arrowVectors = arrowSchema.zip(_root.getFieldVectors.asScala).toMap
    schema.fields.zipWithIndex.foreach { case (field, index) =>
      val dataType = indexedSchema(index)
      val fieldName = MetadataFields.getOriginalName(field)
      val arrowVector = arrowVectors.get(fieldName)
        .map { vec =>
          val columnSchema = ytSchema.getColumnSchema(ytSchema.findColumn(fieldName))
          if (columnSchema == null) {
            throw new IllegalStateException(s"Column $fieldName not found in schema")
          }
          createArrowColumnVector(vec, dataType, columnSchema.getType)
        }
        .getOrElse(ArrowColumnVector.nullVector(dataType))
      _columnVectors(index) = arrowVector
    }
    _batch = new ColumnarBatch(_columnVectors)
  }
}
