package es.us.idea.dataQuality.internal.dimension

import es.us.idea.dataQuality.internal.businessRules.BusinessRule
import es.us.idea.dataQuality.internal.decisionRules.DecisionRulesEngine

class CompletenessDQDimension(
                               weight: Double,
                               businessRules: Seq[BusinessRule],
                               decisionRules: Option[DecisionRulesEngine] = None
                             ) extends AbstractDQDimension(weight, businessRules, decisionRules) {
  def this(businessRules: => Seq[BusinessRule]) = this(1.0, businessRules)
}
