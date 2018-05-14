package es.us.idea.dataQuality.internal.conditions.valConditions.numeric

class LessEqThan(key: String, values: Seq[Any]) extends AbstractNumericCondition(key, values, let) {
  def this(key: String, value: Any) = this(key, Seq(value))
}