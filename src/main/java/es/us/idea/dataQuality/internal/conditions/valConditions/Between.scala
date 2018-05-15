package es.us.idea.dataQuality.internal.conditions.valConditions

import es.us.idea.dataQuality.Utils

class Between(key: Any, lowerBound: Any, upperBound: Any) extends ValueCondition(key, Seq(lowerBound, upperBound)){
  override def operation(value: Any): Boolean = {
    if(Utils.toDouble(value).isEmpty) return false

    val lowerBoundDouble = Utils.toDouble(lowerBound)
    val upperBoundDouble = Utils.toDouble(upperBound)

    if(lowerBoundDouble.isEmpty || upperBoundDouble.isEmpty) return false

    lowerBoundDouble.get <= Utils.toDouble(value).get && upperBoundDouble.get >= Utils.toDouble(value).get
  }
}
