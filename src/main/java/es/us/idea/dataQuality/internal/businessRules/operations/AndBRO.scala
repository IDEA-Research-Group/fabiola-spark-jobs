package es.us.idea.dataQuality.internal.businessRules.operations

import es.us.idea.dataQuality.internal.businessRules.BusinessRule

class AndBRO (businessRules: Seq[BusinessRule]) extends AbstractBusinessRuleOperation {
  def operation(in: Map[String, Any]): Int = businessRules.map(_.getMetric(in)).sum / businessRules.length
}

