package es.us.idea.mapping

import scala.util.Try

object Utils {
  def getValueOfKey(key: String, map: Map[String, Any]): Option[Any] = {
    if (key.contains(".")) {
      val keySplt = key.split('.')
      if (map.contains(keySplt.head)) {
        return getValueOfKey(keySplt.tail.mkString("."), map.get(keySplt.head).get.asInstanceOf[Map[String, Any]])
      } else {
        return null
      }
    } else {
      return map.get(key) // .getOrElse(null)
    }
  }

  def asDouble(value: Any): Option[Double] = {
    value match {
      case s: String => Try(s.toDouble).toOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }

  def asInt(value: Any): Option[Int] = Try(asDouble(value).get.toInt).toOption

}
