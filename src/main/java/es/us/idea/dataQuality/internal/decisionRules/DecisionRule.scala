package es.us.idea.dataQuality.internal.decisionRules

import es.us.idea.dataQuality.internal.conditions.Condition

class DecisionRule(condition: Condition, value: String) extends DecisionRuleTrait {
  def getValue(input: Option[Map[String, Any]]): Option[String] = {
    if(condition.evaluate(input)) Option(value) else None
  }
}
