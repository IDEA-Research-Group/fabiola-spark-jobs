package es.us.idea.mapping.internal

import es.us.idea.mapping.internal.types.GenericData

sealed trait DataMapTrait {
  def getValue(in: Map[String, Any]): GenericData
}

abstract class DataMap(to: String) extends DataMapTrait {

}
