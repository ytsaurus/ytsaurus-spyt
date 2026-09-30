package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.{YtDataSourceV2, YtTable}

import tech.ytsaurus.spyt.format.bucketing.YtBucketingCatalog.{BucketFunction, bucketFunctionName, reservedCatalogName}
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration.HashBucketingCatalogRead
import tech.ytsaurus.spyt.types.YTsaurusTypes
import tech.ytsaurus.typeinfo.TiType

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._

final class YtBucketingCatalog extends TableCatalog with FunctionCatalog {
  private var catalogName: String = _

  override def name(): String = {
    if (catalogName == null) {
      throw new IllegalStateException("YtBucketingCatalog.name() was called before initialize()")
    }
    catalogName
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    require(name != null && name.nonEmpty, s"YtBucketingCatalog needs a non-empty catalog name, got '$name'")
    require(
      !name.equalsIgnoreCase(reservedCatalogName),
      s"YtBucketingCatalog cannot be registered as '$name': SQL reads such as $reservedCatalogName.`//path` " +
        "resolve that name to the YTsaurus data source, so register the catalog under another name")
    catalogName = name
  }

  override def loadTable(ident: Identifier): Table = {
    if (ident.namespace().nonEmpty) {
      throw new NoSuchTableException(ident)
    }
    val source = new YtDataSourceV2()
    val catalogOptions = Map("path" -> ident.name(), HashBucketingCatalogRead.name -> "true")
    source.getTable(YtTable.mergeScanOptions(
      new CaseInsensitiveStringMap(source.sessionOptions.asJava),
      new CaseInsensitiveStringMap(catalogOptions.asJava)))
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty

  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: JMap[String, String]): Table = throw unsupported("CREATE TABLE", ident.toString)

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw unsupported("ALTER TABLE", ident.toString)
  }

  override def dropTable(ident: Identifier): Boolean = throw unsupported("DROP TABLE", ident.toString)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw unsupported("RENAME TABLE", s"$oldIdent to $newIdent")
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty) Array(Identifier.of(namespace, bucketFunctionName)) else Array.empty
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    if (ident.namespace().isEmpty && ident.name() == bucketFunctionName) {
      BucketFunction
    } else {
      throw new NoSuchFunctionException(ident)
    }
  }

  private def unsupported(operation: String, target: String): UnsupportedOperationException = {
    new UnsupportedOperationException(s"$operation is not supported for $target by the '${name()}' catalog")
  }
}

/**
 * The bucket function of YTsaurus tables bucketed by a `farm_hash(column) % N` computed key column.
 *
 * Exact hash compatibility: Spark 4.x one-side shuffle evaluates the bound function on the Catalyst values of the
 * side that is not bucketed and routes each row by the result. A value that hashes differently from the bucket
 * YTsaurus stored for it lands in the wrong partition, and the join silently loses its matches.
 *
 * Supported argument types: integral types (uint64 only as the extended UInt64Type), boolean, UTF8_BINARY string
 * (YTsaurus string, utf8 and json), date, timestamp and interval, which is read as bigint.
 *
 * Unsupported types: bind refuses decimal (including uint64 read as decimal(20,0)), datetime and the 64-bit date and
 * time user types, binary, strings with another collation and calls with several columns by throwing
 * UnsupportedOperationException. Spark then drops the bucket partitioning of the scan and shuffles. A uuid argument
 * binds as a string, but the scan does not report bucketing for it, since its Spark text is not the bytes YTsaurus
 * hashes.
 *
 * Catalyst values: produceResult hashes the internal value of the argument, that is UTF8String for a string, the
 * day number (Int) for a date, microseconds (Long) for a timestamp and the underlying Long for UInt64Type.
 */
object YtBucketingCatalog {
  /**
   * Fixed by Spark, not a free choice. YtScan reports its partitioning as the standard BucketTransform, whose name is
   * always "bucket", and V2ScanPartitioningAndOrdering resolves that transform by loading the function of the same
   * name from the empty namespace of the table's catalog. Under any other name the lookup fails, Spark drops the scan
   * partitioning with only a warning and the join silently shuffles. The two join sides are matched by canonicalName.
   */
  val bucketFunctionName = "bucket"
  /**
   * The name spark-defaults.conf registers the catalog under. Any other name works the same, since bucketing depends
   * on the catalog a table is read through, not on its name, and the two sides of a join are matched by the
   * canonical name of the bound function; another name helps when ytsaurus is taken by another catalog or existing
   * queries already use a different one.
   */
  val defaultCatalogName = "ytsaurus"

  /** Refused as a catalog name, since SQL reads by path such as yt.`//path` already resolve it. */
  val reservedCatalogName = "yt"

  private val supportedValueTypes: Set[DataType] =
    Set(LongType, IntegerType, ShortType, ByteType, BooleanType, StringType, DateType, TimestampType)

  private lazy val nativeUInt64Type: Option[DataType] = YTsaurusTypes.instance.sparkTypeFor(TiType.uint64()) match {
    case _: DecimalType => None
    case uint64Type => Some(uint64Type)
  }

  /** Spark types of a bucket column whose values the bucket function hashes exactly as YTsaurus farm_hash does. */
  def isSupportedArgumentType(dataType: DataType): Boolean = {
    supportedValueTypes.contains(dataType) || nativeUInt64Type.contains(dataType)
  }

  object BucketFunction extends UnboundFunction {
    override def name(): String = bucketFunctionName

    override def description(): String =
      s"$bucketFunctionName(int, value): YTsaurus farm_hash(value) % buckets of one integral, boolean, string, " +
        "date or timestamp value"

    override def bind(inputType: StructType): BoundFunction = inputType.fields match {
      case Array(bucketCount, argument)
        if bucketCount.dataType == IntegerType && isSupportedArgumentType(argument.dataType) =>
        BoundBucketFunction(argument.dataType)
      case fields =>
        throw new UnsupportedOperationException(s"$bucketFunctionName is supported for an int bucket count and " +
          "a single integral, boolean, string, date or timestamp argument only, got " +
          fields.map(_.dataType.catalogString).mkString("[", ", ", "]"))
    }
  }

  /**
   * farm_hash(value) % buckets as YTsaurus stores it: a NULL value hashes as YTsaurus NULL, so the result is never
   * NULL. There is no magic invoke method on purpose, since Spark returns NULL for a NULL argument of a primitive
   * parameter. A NULL or non-positive bucket count throws IllegalArgumentException; Spark always passes the table's
   * bucket count.
   */
  final case class BoundBucketFunction(argumentType: DataType) extends ScalarFunction[java.lang.Long] {
    override def name(): String = bucketFunctionName

    override def canonicalName(): String = s"spyt.farm_hash_bucket(int, ${argumentType.catalogString})"

    override def inputTypes(): Array[DataType] = Array(IntegerType, argumentType)

    override def resultType(): DataType = LongType

    override def isResultNullable: Boolean = false

    override def produceResult(input: InternalRow): java.lang.Long = {
      if (input.isNullAt(0)) {
        throw new IllegalArgumentException(s"$bucketFunctionName bucket count must not be null")
      }
      val value = if (input.isNullAt(1)) null else input.get(1, argumentType)
      HashFunctionCall.bucketOf(HashFunction.FARM_HASH.hash(value), input.getInt(0))
    }
  }
}
