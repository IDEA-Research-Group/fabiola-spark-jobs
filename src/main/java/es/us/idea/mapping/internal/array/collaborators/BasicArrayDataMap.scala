package es.us.idea.mapping.internal.array.collaborators

import es.us.idea.mapping.Utils

import scala.util.Try

class BasicArrayDataMap(fromArray: String) extends ArrayDataMapTrait {
  // Can throw an exception if it cannot cast
  override def getValue(value: Any): Option[Seq[Option[Any]]] = {
    val map = Try(value.asInstanceOf[Map[String, Any]]).toOption
    if(map.isDefined) Try(Utils.getValueOfKey(fromArray, map.get).get.asInstanceOf[Seq[Any]].map(Option(_))).toOption
    else None
  }
}
