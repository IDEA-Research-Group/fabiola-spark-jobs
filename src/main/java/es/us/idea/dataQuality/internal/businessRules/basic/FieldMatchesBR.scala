package es.us.idea.dataQuality.internal.businessRules.basic

class FieldMatchesBR(field: String, values: Seq[Any]) extends AbstractBusinessRule(field, values) {
  def operation(fieldValue: Any): Int = if(values contains fieldValue) 1 else 0
}
