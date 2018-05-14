package es.us.idea.dataQuality.internal.conditions.valConditions.numeric

import es.us.idea.dataQuality.Utils
import es.us.idea.dataQuality.internal.conditions.valConditions.ValueCondition
import es.us.idea.dataQuality.internal.conditions.valConditions.numeric

abstract class AbstractNumericCondition(key: Any, values: Seq[Any], op: Operations) extends ValueCondition(key, values) {

  override def operation(value: Any): Boolean = {
    if (Utils.toDouble(value).isEmpty) return false
    values.count(f(Utils.toDouble(value).get, _, op)) > 0
  }

  private def f(fieldValue: Double, iteratorValue: Any, op: Operations): Boolean = {
    if (Utils.toDouble(iteratorValue).isEmpty) return false
    op match {
      case numeric.gt => return fieldValue > Utils.toDouble(iteratorValue).get
      case numeric.get => return fieldValue >= Utils.toDouble(iteratorValue).get
      case numeric.lt => return fieldValue < Utils.toDouble(iteratorValue).get
      case numeric.let => return fieldValue <= Utils.toDouble(iteratorValue).get
      case numeric.between => return fieldValue <= Utils.toDouble(iteratorValue).get
      case _ => return false
    }
  }

}
