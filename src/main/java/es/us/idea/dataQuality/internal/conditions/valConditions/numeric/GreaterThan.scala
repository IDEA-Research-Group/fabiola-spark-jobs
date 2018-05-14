package es.us.idea.dataQuality.internal.conditions.valConditions.numeric

class GreaterThan(key: String, values: Seq[Any]) extends AbstractNumericCondition(key, values, gt) {
  def this(key: String, value: Any) = this(key, Seq(value))
}