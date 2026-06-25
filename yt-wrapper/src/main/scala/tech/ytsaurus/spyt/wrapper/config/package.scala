package tech.ytsaurus.spyt.wrapper

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SQLContext, SparkSession}
import tech.ytsaurus.ysontree.{YTreeNode, YTreeTextSerializer}

import java.util.{Map => JMap}
import java.util.Properties
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Try

package object config {

  trait ConfProvider {
    def getYtConf(name: String): Option[String]

    def getAllKeys: Seq[String]

    def getYtConf[T](conf: ConfigEntry[T]): Option[T] = {
      @tailrec
      def findFirst(names: List[String]): Option[String] = names match {
        case head :: tail =>
          val someValue = getYtConf(head)
          if (someValue.isEmpty) findFirst(tail) else someValue
        case Nil => None
      }

      conf.get(findFirst(conf.name :: conf.aliases))
    }

    def ytConf[T](conf: ConfigEntry[T]): T = {
      getYtConf(conf).get
    }

    def ytConf[T](conf: MultiConfigEntry[T]): Map[String, T] = {
      conf.get(getAllKeys, this)
    }
  }

  private def dropPrefix(seq: Seq[String], prefix: String): Seq[String] = {
    val fullPrefix = s"$prefix."
    seq.collect {
      case str if str.startsWith(fullPrefix) => str.drop(fullPrefix.length)
    }
  }

  // Resolves a config by trying each prefix in order; spark.ytsaurus is preferred, spark.yt kept as a fallback alias.
  trait PrefixedConfProvider extends ConfProvider {
    protected def configurationPrefixes: Seq[String]
    protected def rawGet(key: String): Option[String]
    protected def rawKeys: Seq[String]

    override def getYtConf(name: String): Option[String] =
      configurationPrefixes.flatMap(prefix => rawGet(s"$prefix.$name")).headOption

    override def getAllKeys: Seq[String] =
      configurationPrefixes.flatMap(prefix => dropPrefix(rawKeys, prefix)).distinct
  }

  // Writes go to the primary prefix (configurationPrefixes.head).
  trait WritableConfProvider extends PrefixedConfProvider {
    protected def rawSet(key: String, value: String): Unit

    def setYtConf(name: String, value: Any): Unit =
      rawSet(s"${configurationPrefixes.head}.$name", value.toString)

    def setYtConf[T](configEntry: ConfigEntry[T], value: T): Unit =
      setYtConf(configEntry.name, configEntry.set(value))
  }

  implicit class SparkYtSqlContext(sqlContext: SQLContext) extends PrefixedConfProvider {
    override protected val configurationPrefixes = Seq("spark.ytsaurus", "spark.yt")
    override protected def rawGet(key: String): Option[String] = Try(sqlContext.getConf(key)).toOption
    override protected def rawKeys: Seq[String] = sqlContext.sparkSession.conf.getAll.keys.toList
  }

  implicit class SparkYtSqlConf(sqlConf: SQLConf) extends PrefixedConfProvider {
    override protected val configurationPrefixes = Seq("spark.hadoop.yt")
    override protected def rawGet(key: String): Option[String] =
      if (sqlConf.contains(key)) Some(sqlConf.getConfString(key)) else None
    override protected def rawKeys: Seq[String] = sqlConf.getAllDefinedConfs.map(_._1).toSeq
  }

  implicit class SparkYtSparkConf(sparkConf: SparkConf) extends PrefixedConfProvider {
    override protected val configurationPrefixes = Seq("spark.ytsaurus", "spark.yt")
    override protected def rawGet(key: String): Option[String] = sparkConf.getOption(key)
    override protected def rawKeys: Seq[String] = sparkConf.getAll.map(_._1).toSeq

    def setYtConf(name: String, value: Any): SparkConf = {
      sparkConf.set(s"${configurationPrefixes.head}.$name", value.toString)
    }

    def setYtConf[T](configEntry: ConfigEntry[T], value: T): SparkConf = {
      setYtConf(configEntry.name, configEntry.set(value))
    }
  }

  implicit class SparkYtSparkSession(spark: SparkSession) extends WritableConfProvider {
    override protected val configurationPrefixes = Seq("spark.ytsaurus", "spark.yt")
    override protected def rawGet(key: String): Option[String] = spark.conf.getOption(key)
    override protected def rawKeys: Seq[String] = spark.conf.getAll.keys.toList
    override protected def rawSet(key: String, value: String): Unit = spark.conf.set(key, value)
  }

  implicit class SparkYtHadoopConfiguration(configuration: Configuration) extends WritableConfProvider {
    override protected val configurationPrefixes = Seq("yt")
    override protected def rawGet(key: String): Option[String] = Option(configuration.get(key))
    override protected def rawKeys: Seq[String] = configuration.asScala.map(_.getKey).toList
    override protected def rawSet(key: String, value: String): Unit = configuration.set(key, value)

    def getYtSpecConf(name: String): Map[String, YTreeNode] = {
      configuration.asScala.collect {
        case entry if entry.getKey.startsWith(s"spark.yt.$name") =>
          val key = entry.getKey.drop(s"spark.yt.$name.".length)
          val value = YTreeTextSerializer.deserialize(entry.getValue)
          key -> value
      }.toMap
    }

    def getConfWithPrefix(prefix: String): JMap[String, String] = {
      val fullPrefix = s"${configurationPrefixes.head}.$prefix."
      configuration.getPropsWithPrefix(fullPrefix)
    }
  }

  implicit class PropertiesConf(props: Properties) extends ConfProvider {
    override def getYtConf(name: String): Option[String] = Option(props.getProperty(name))
    override def getAllKeys: Seq[String] = props.stringPropertyNames().asScala.toSeq
  }


  implicit class OptionsConf(options: Map[String, String]) extends ConfProvider {
    override def getYtConf(name: String): Option[String] = options.get(name)

    override def getAllKeys: Seq[String] = options.keys.toList
  }

  implicit class CaseInsensitiveMapConf(options: CaseInsensitiveStringMap) extends ConfProvider {
    override def getYtConf(name: String): Option[String] = if (options.containsKey(name)) {
      Some(options.get(name))
    } else None

    override def getAllKeys: Seq[String] = {
      options.keySet().asScala.toList
    }
  }
}
