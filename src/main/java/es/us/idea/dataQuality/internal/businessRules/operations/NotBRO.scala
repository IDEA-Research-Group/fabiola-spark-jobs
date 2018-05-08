package es.us.idea.dataQuality.internal.businessRules.operations

import es.us.idea.dataQuality.internal.businessRules.BusinessRule

class NotBRO(businessRule: BusinessRule) extends AbstractBusinessRuleOperation {
  def operation(in: Map[String, Any]): Int = 1 - businessRule.getMetric(in)
}
