package es.us.idea.mapping.internal.array

import es.us.idea.mapping.Utils

class FieldsToArrayDataMap(fromFields: Seq[String]) extends ArrayDataMapTrait {
  override def getValue(value: Any): Seq[Any] = {
    val map = value.asInstanceOf[Map[String, Any]]
    fromFields.map(x => Utils.getValueOfKey(x, map))
  }
}
