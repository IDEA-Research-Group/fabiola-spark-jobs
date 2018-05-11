package es.us.idea.dataQuality.internal.dimension

import es.us.idea.dataQuality.Utils
import es.us.idea.dataQuality.internal.businessRules.BusinessRule

class CustomDQDimension(name: String, weightedBusinessRules: Seq[(Double, BusinessRule)]) extends AbstractDQDimension(weightedBusinessRules) {
  def this(name: String, businessRules: => Seq[BusinessRule]) = this(name, Utils.getDefaultWeightsBusinessRules(businessRules))
}
