package es.us.idea.dataQuality.internal.conditions.valConditions

class NotNull(key: Any) extends Matches(key, Seq(null)) {
  override def operation(value: Any): Boolean = !super.operation(value)
}
