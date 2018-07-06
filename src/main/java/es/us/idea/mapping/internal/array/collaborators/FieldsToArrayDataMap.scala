package es.us.idea.mapping.internal.array.collaborators

import es.us.idea.mapping.Utils

import scala.util.Try

class FieldsToArrayDataMap(fromFields: Seq[String]) extends ArrayDataMapTrait {
  // Can throw an exception if it cannot cast
  override def getValue(value: Any): Option[Seq[Option[Any]]] = {
    val map = Try(value.asInstanceOf[Map[String, Any]]).toOption
    if(map.isDefined) Try(fromFields.map(x => Utils.getValueOfKey(x, map.get))).toOption
    else None
  }
}
