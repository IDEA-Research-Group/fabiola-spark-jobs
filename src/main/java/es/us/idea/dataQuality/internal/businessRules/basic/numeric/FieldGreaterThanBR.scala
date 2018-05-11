package es.us.idea.dataQuality.internal.businessRules.basic.numeric

class FieldGreaterThanBR(field: String, values: Seq[Any]) extends AbstractNumericFieldBR(field, values, gt) {
}
