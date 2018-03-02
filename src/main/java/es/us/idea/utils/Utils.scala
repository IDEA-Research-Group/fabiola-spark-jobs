package es.us.idea.utils

import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

object Utils {

  def jsonToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = DefaultFormats
    JsonMethods.parse(jsonStr).extract[Map[String, Any]]
  }

  def mapToJson(map: Map[String, Any]): String = {
    implicit val formats = DefaultFormats
    Serialization.write(map)
  }

  def calculateOptimization(in: Map[String, Any], currentObjectiveValue: String): Map[String, Any] = {
    if (in.exists(_._1 == "optimal") && in.exists(_._1 == currentObjectiveValue)) {
      val current = in.get(currentObjectiveValue).get.asInstanceOf[Double]
      val optimal = in.get("optimal").get.asInstanceOf[Double]
      Map("optimization" -> ((optimal - current) / current) * 100.0)
    } else {
      Map("optimization" -> 0.0)
    }
  }

  def removeLastSlashes(input: String): String = {
    if (input.last.equals("/")) removeLastSlashes(input.dropRight(1)) else input
  }

}
