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
}
