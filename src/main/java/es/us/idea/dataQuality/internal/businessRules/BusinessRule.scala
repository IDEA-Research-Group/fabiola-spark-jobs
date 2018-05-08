package es.us.idea.dataQuality.internal.businessRules

trait BusinessRule {
  def getMetric(in: Map[String, Any]): Int
}
