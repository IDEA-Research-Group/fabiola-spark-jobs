package es.us.idea.dataQuality.internal.businessRules.operations

import es.us.idea.dataQuality.internal.businessRules.BusinessRule

class IfBRO(condition: BusinessRule, consequence: BusinessRule, default: Boolean) extends AbstractBusinessRuleOperation {
  override def operation(in: Map[String, Any]): Int = {
    if(condition.evaluate(in)) consequence.getMetric(in) else if(default) 1 else 0
  }
}
