package es.us.idea.dataQuality.internal.conditions.valConditions.numeric

class GreaterEqThan(key: String, values: Seq[Any]) extends AbstractNumericCondition(key, values, get) {
  def this(key: String, value: Any) = this(key, Seq(value))
}