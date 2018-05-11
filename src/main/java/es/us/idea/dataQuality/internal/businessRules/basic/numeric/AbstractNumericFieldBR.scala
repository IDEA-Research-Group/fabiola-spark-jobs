package es.us.idea.dataQuality.internal.businessRules.basic.numeric

import es.us.idea.dataQuality.Utils
import es.us.idea.dataQuality.internal.businessRules.basic.{AbstractBusinessRule, numeric}

abstract class AbstractNumericFieldBR(field: String, values: Seq[Any], op: Operations) extends AbstractBusinessRule(field, values) {


  override def operation(fieldValue: Any): Int = {
    if (Utils.toDouble(fieldValue).isEmpty) return 0
    if (values.count(f(Utils.toDouble(fieldValue).get, _, op)) > 0) 1 else 0
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
