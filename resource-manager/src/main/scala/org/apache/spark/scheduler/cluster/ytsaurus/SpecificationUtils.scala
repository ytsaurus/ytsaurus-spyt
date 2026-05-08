package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.deploy.ytsaurus.Config._
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager.DRIVER_TASK
import tech.ytsaurus.ysontree._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try


object SpecificationUtils {
  private def createNestedStructure(allSettings: Map[String, String]): Map[String, Any] = {

    def processValue(value: String): Any = {
      if (value.contains(",")) {
        return value.split(",")
          .map(s => convertSimpleValue(s.trim))
          .toList
      }
      convertSimpleValue(value)
    }

    def convertSimpleValue(s: String): Any = {
      Try(s.toBoolean).getOrElse(
        Try(s.toLong).getOrElse(
          Try(s.toDouble).getOrElse(s)
        )
      )
    }

    def convertValue(value: Any): Any = value match {
      case b: Boolean => b
      case l: Long => l
      case d: Double => d
      case _ => processValue(value.toString)
    }

    def nestedMap(path: List[String], value: Any, currentMap: mutable.Map[String, Any]): mutable.Map[String, Any] = {
      path match {
        case head :: Nil =>
          currentMap += (head -> convertValue(value))
        case head :: tail =>
          val nextMap = currentMap.getOrElse(head, mutable.Map[String, Any]()).asInstanceOf[mutable.Map[String, Any]]
          currentMap += (head -> nestedMap(tail, value, nextMap))
        case _ =>
          currentMap
      }
    }

    val result = mutable.Map[String, Any]()
    allSettings.foreach { case (key, value) =>
      val parts = key.split("\\.")
      nestedMap(parts.toList, value, result)
    }
    result.toMap
  }

  private[ytsaurus] def convertToYTree(data: Any): Any = {
    data match {
      case mutableMap: mutable.Map[String, Any] @unchecked => convertToYTree(mutableMap.toMap)
      case map: Map[String, Any] @unchecked=>
        val builder = YTree.mapBuilder()
        map.foreach {
          case (key, value) =>
            builder.key(key).value(convertToYTree(value))
        }
        builder.buildMap()

      case list: List[Any] =>
        val builder = YTree.listBuilder()
        list.foreach(value => builder.value(convertToYTree(value)))
        builder.buildList()

      case bool: Boolean => YTree.booleanNode(bool)
      case long: Long => YTree.longNode(long)
      case double: Double => YTree.doubleNode(double)
      case other => YTree.stringNode(other.toString)
    }
  }

  private[ytsaurus] def convertToScalaMap(data: YTreeNode): Any = {

    data match {
      case map: YTreeMapNode =>
        (for (key <- map.keys.asScala) yield {
          key -> convertToScalaMap(map.get(key).get())
        }).toMap
      case list: YTreeListNode =>
        list.asList().asScala.map(convertToScalaMap).toList
      case boolNode: YTreeBooleanNode => boolNode.boolValue()
      case longNode: YTreeIntegerNode => longNode.longValue()
      case doubleNode: YTreeDoubleNode => doubleNode.doubleValue()
      case other: YTreeNode => other.stringValue()
    }
  }

  private[ytsaurus] def getAnnotationsAsMap(conf: SparkConf, isDriver: Boolean): Map[String, Any] = {
    val validPrefixes = if (isDriver)
      List(SPYT_ANNOTATIONS, SPYT_DRIVER_ANNOTATIONS)
    else List(SPYT_ANNOTATIONS, SPYT_EXECUTORS_ANNOTATIONS)

    val filteredConf = validPrefixes.flatMap(prefix => conf.getAllWithPrefix(prefix)).filter {
      case (key, _) => key != null && key.nonEmpty
    }
    val configAsMapTree = createNestedStructure(filteredConf.toMap).values.headOption match {
      case Some(map: mutable.HashMap[String, Any] @unchecked) => map.toMap
      case Some(map: Map[String, Any] @unchecked) => map
      case None => Map.empty[String, Any]
      case _ => throw new IllegalArgumentException("Unexpected map type stored in annotations")
    }
    configAsMapTree
  }

  private[ytsaurus] def getAnnotationsAsYTreeMapNode(conf: SparkConf, taskName: String): YTreeMapNode = {
    val annotationsMapFromPlainForm: Map[String, Any] = getAnnotationsAsMap(conf, taskName == DRIVER_TASK)
    val prefix = s"spark.ytsaurus.$taskName.operation.parameters"
    if (conf.contains(prefix)) {
      val params = conf.get(prefix)
      val paramsTree: YTreeMapNode = YTreeTextSerializer.deserialize(params).mapNode()
      if (paramsTree.containsKey("annotations")) {
        val annotationsMapFromYsonForm: Map[String, Any] = convertToScalaMap(paramsTree.get("annotations").get()).asInstanceOf[Map[String, Any]]
        val annotationsMap = mergeMaps(annotationsMapFromPlainForm, annotationsMapFromYsonForm)
        return convertToYTree(annotationsMap).asInstanceOf[YTreeMapNode]
      }
    }
    convertToYTree(annotationsMapFromPlainForm).asInstanceOf[YTreeMapNode]
  }

  private def mergeMaps(map1: Map[String, Any], map2: Map[String, Any]): Map[String, Any] = {
    val result = mutable.Map[String, Any]() ++= map1

    map2.foreach { case (key, map2Value) =>
      val map1Value = if (result.contains(key) && result(key).isInstanceOf[mutable.HashMap[String @unchecked, Any @unchecked]])
        result(key).asInstanceOf[mutable.HashMap[String, Any]].toMap
      else result.get(key)

      result(key) = (map1Value, map2Value) match {
        case (existing: Map[_, _], incoming: Map[_, _]) =>
          mergeMaps(existing.asInstanceOf[Map[String, Any]], incoming.asInstanceOf[Map[String, Any]])
        case (Some(existing: List[_]), incoming: List[_]) =>
          existing ++ incoming
        case (Some(existing: Any), _) =>
          existing
        case (_, _) =>
          map2Value
      }
    }
    result.toMap
  }
}
