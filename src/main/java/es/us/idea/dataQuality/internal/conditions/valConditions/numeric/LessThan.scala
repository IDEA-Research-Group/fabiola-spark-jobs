package es.us.idea.dataQuality.internal.conditions.valConditions.numeric

class LessThan(key: String, values: Seq[Any]) extends AbstractNumericCondition(key, values, lt) {
  def this(key: String, value: Any) = this(key, Seq(value))
}