package es.us.idea.dataQuality.internal.dimension

import es.us.idea.dataQuality.Utils
import es.us.idea.dataQuality.internal.businessRules.BusinessRule

class ConsistencyDQDimension(weightedBusinessRules: Seq[(Double, BusinessRule)]) extends AbstractDQDimension(weightedBusinessRules) {
  def this(businessRules: => Seq[BusinessRule]) = this(Utils.getDefaultWeightsBusinessRules(businessRules))
}
