package es.us.idea.dataQuality

import scala.util.Try

object Utils {
  // Child classes constructor helper
//  def getDefaultWeightsBusinessRules(businessRules: Seq[BusinessRule]): Seq[(Double, BusinessRule)] =
//    businessRules.map((1.0 / businessRules.length, _))

  def toDouble: (Any) => Option[Double] = {
    case s: String => Try(s.toDouble).toOption
    case i: Int => Option(i)
    case l: Long => Option(l)
    case f: Float => Option(f)
    case d: Double => Option(d)
    case _ => None
  }

  def getValueOfKey(key: String, map: Map[String, Any]): Any = {
    if (key.contains(".")) {
      val keySplt = key.split('.')
      if (map.contains(keySplt.head)) {
        return getValueOfKey(keySplt.tail.mkString("."), map.get(keySplt.head).get.asInstanceOf[Map[String, Any]])
      } else {
        return null
      }
    } else {
      return map.get(key).getOrElse(null)
    }
  }
}
