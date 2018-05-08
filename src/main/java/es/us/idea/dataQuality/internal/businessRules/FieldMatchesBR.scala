package es.us.idea.dataQuality.internal.businessRules

class FieldMatchesBR(field: String, values: Seq[String]) extends AbstractBusinessRule(field, values) {
  def operation(fieldValue: Any): Int = if(values contains fieldValue) 1 else 0
}
