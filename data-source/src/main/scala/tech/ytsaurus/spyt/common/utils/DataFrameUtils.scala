package tech.ytsaurus.spyt.common.utils

import java.util.{ArrayList => JList}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.SparkAdapter

import scala.reflect.runtime.universe.TypeTag

object DataFrameUtils {

  implicit class UtilsDataFrame(df: DataFrame) {
    private val shufflePartitions = df.sparkSession.sqlContext.getConf("spark.sql.shuffle.partitions").toInt

    def top(groupBy: Seq[String], topBy: Seq[String], partitions: Int = shufflePartitions): DataFrame = {
      val sortCols = groupBy ++ topBy
      df
        .repartition(partitions, groupBy.map(col): _*)
        .sortWithinPartitions(sortCols.head, sortCols.tail: _*)
        .mapPartitions { rows =>
          var currentKey = Option.empty[Seq[String]]
          rows.flatMap { row =>
            val key = groupBy.map(row.getAs[String])
            if (!currentKey.contains(key)) {
              currentKey = Some(key)
              Some(row)
            } else {
              None
            }
          }
        }(SparkAdapter.instance.createExpressionEncoder(df.schema))
    }

    def joinWithHotKey(right: DataFrame, key: String, hotKey: Option[String], joinType: String, andCondition: Column = lit(true)): DataFrame = {
      import df.sparkSession.implicits._

      val splitHotDf = (1 to 1000).map(i => hotKey -> s"#null$i").toDF(key, s"${key}_key")

      val randomSplit = concat(lit("#null"), floor(rand() * 1000 + 1))

      val keyIsHot = hotKey match {
        case None => col(key).isNull
        case Some(value) => col(key) === value
      }

      val splitLeft = df
        .withColumn(s"${key}_key", when(keyIsHot, randomSplit).otherwise(col(key)))

      val splitRight = right
        .join(broadcast(splitHotDf), right(key) <=> splitHotDf(key), "left_outer")
        .drop(splitHotDf(key))
        .withColumn(s"${key}_key", coalesce(col(s"${key}_key"), col(key)))

      splitLeft
        .join(splitRight, splitLeft(s"${key}_key") === splitRight(s"${key}_key") && andCondition, joinType)
        .drop(splitLeft(s"${key}_key"))
        .drop(splitRight(key))
        .drop(s"${key}_key")
    }

    type MinBy = (String, Seq[String])

    def minByColumns(groupBy: String,
                     minBy: Seq[MinBy],
                     maxBy: Seq[MinBy],
                     outputSchema: StructType): DataFrame = {
      import df.sparkSession.implicits._

      df
        .groupByKey(_.getAs[String](groupBy))
        .mapGroups { case (id, rows) =>
          val collected = rows.toList

          Row.fromSeq(
            (id +: minBy.flatMap { case (minByName, fields) =>
              val minRow = minByNoneLast(collected)(_.getAsOption[String](minByName))
              fields.map(minRow.getAs[Any])
            }) ++ maxBy.flatMap { case (maxByName, fields) =>
              val maxRow = collected.maxBy(_.getAsOption[String](maxByName))
              fields.map(maxRow.getAs[Any])
            }
          )
        }(SparkAdapter.instance.createExpressionEncoder(outputSchema))
    }

    def selectAs[T <: Product : TypeTag]: Dataset[T] = {
      import df.sparkSession.implicits._
      val fields = Encoders.product[T].schema.fieldNames
      df.select(fields.head, fields.tail: _*).as[T]
    }
  }

  implicit class UtilsRow(row: Row) {
    def getAsOption[T](name: String): Option[T] = {
      if (row.isNullAt(row.fieldIndex(name))) {
        None
      } else {
        Some(row.getAs[T](name))
      }
    }
  }

  def getDataFrameTop(df: DataFrame, groupBy: JList[String], topBy: JList[String], partitions: java.lang.Integer): DataFrame = {
    import scala.collection.JavaConverters._
    df.top(groupBy.asScala, topBy.asScala, partitions)
  }

  def joinWithHotKeyNull(left: DataFrame, right: DataFrame, key: String, joinType: String, andCondition: Column): DataFrame = {
    left.joinWithHotKey(right, key, None, joinType, andCondition)
  }

  def minByColumns(df: DataFrame, groupBy: String,
                   minBy: JList[JList[String]],
                   maxBy: JList[JList[String]]): DataFrame = {
    import scala.collection.JavaConverters._
    val minByScala = minBy.asScala.map { v =>
      val asScala = v.asScala
      asScala.head -> asScala
    }
    val maxByScala = maxBy.asScala.map { v =>
      val asScala = v.asScala
      asScala.head -> asScala
    }
    df.minByColumns(groupBy, minByScala, maxByScala, df.schema)
  }

  def minByNoneLast[A, B](seq: Seq[A])(f: A => Option[B])(implicit ordering: Ordering[B]): A = {
    seq.minBy { a =>
      val b = f(a)
      b.isEmpty -> b
    }
  }
}
