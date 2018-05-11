package es.us.idea.dataQuality.internal.businessRules.basic

class FieldNotNullBR(field: String) extends FieldMatchesBR(field, Seq(null)) {
  override def operation(fieldValue: Any): Int = 1 - super.operation(fieldValue) // TODO llamar al futuro NOT de BusinessRuleOperations
}
