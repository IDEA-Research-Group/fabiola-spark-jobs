package es.us.idea.dataQuality.internal.conditions.valConditions

import es.us.idea.dataQuality.Utils

class Between(key: Any, lowerBound: Any, higherBound: Any) extends ValueCondition(key, Seq(lowerBound, higherBound)){
  override def operation(value: Any): Boolean = {
    if(Utils.toDouble(value).isEmpty) return false

    val lowerBoundDouble = Utils.toDouble(lowerBound)
    val higherBoundDouble = Utils.toDouble(higherBound)

    if(lowerBoundDouble.isEmpty || higherBoundDouble.isEmpty) return false

    lowerBoundDouble.get <= Utils.toDouble(value).get && higherBoundDouble.get >= Utils.toDouble(value).get
  }
}
