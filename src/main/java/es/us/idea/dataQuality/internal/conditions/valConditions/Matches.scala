package es.us.idea.dataQuality.internal.conditions.valConditions

class Matches(key: Any, values: Seq[Any]) extends ValueCondition(key, values) {

  def this(key: Any, value: Any) = this(key, Seq(value))

  override def operation(value: Any): Boolean = values contains value
}
