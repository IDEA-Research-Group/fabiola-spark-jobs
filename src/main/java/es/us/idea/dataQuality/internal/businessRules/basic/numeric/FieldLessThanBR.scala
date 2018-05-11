package es.us.idea.dataQuality.internal.businessRules.basic.numeric


class FieldLessThanBR(field: String, values: Seq[Any]) extends AbstractNumericFieldBR(field, values, lt) {
}
