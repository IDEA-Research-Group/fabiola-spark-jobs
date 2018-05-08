package es.us.idea.dataQuality.internal.businessRules

import es.us.idea.dataQuality.DQFunction.getValueOfKey

abstract class AbstractBusinessRule(field: String, values: Seq[String]) extends BusinessRule {

  def operation(fieldValue: Any): Int

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

  def getMetric(in: Map[String, Any]): Int = operation(getValueOfKey(field, in))

}
