package es.us.idea.dataQuality.internal.businessRules.operations

import es.us.idea.dataQuality.internal.businessRules.BusinessRule

abstract class AbstractBusinessRuleOperation extends BusinessRule {
  def operation(in: Map[String, Any]): Int
  def getMetric(in: Map[String, Any]): Int = operation(in)
}
