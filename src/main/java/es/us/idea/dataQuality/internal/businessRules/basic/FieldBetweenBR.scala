package es.us.idea.dataQuality.internal.businessRules.basic

import es.us.idea.dataQuality.Utils

class FieldBetweenBR(field: String, values: Seq[Any]) extends AbstractBusinessRule(field, values) {
  override def operation(fieldValue: Any): Int = {
    if(Utils.toDouble(fieldValue).isEmpty) return 0
    val doubleVals = values.map(Utils.toDouble).filter(_.isDefined)
    if(doubleVals.lengthCompare(2) != 0) return 0
    return if(doubleVals.head.get <= Utils.toDouble(fieldValue).get && doubleVals.last.get >= Utils.toDouble(fieldValue).get) 1 else 0
  }
}
