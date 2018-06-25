package es.us.idea.mapping.internal.types

import scala.util.Try

class GenericData(value: Any) {

  def asDouble(): Option[Double] = {
    value match {
      case s: String => Try(s.toDouble).toOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }
  def asInt(): Option[Int] = Try(asDouble().get.toInt).toOption
  def asString(): Option[String] = Try(value.asInstanceOf[String]).toOption

}
