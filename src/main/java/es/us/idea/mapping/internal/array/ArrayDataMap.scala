package es.us.idea.mapping.internal.array

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal.DataMap
import es.us.idea.mapping.internal.array.collaborators.ArrayDataMapTrait
import es.us.idea.mapping.internal.types.GenericData

class ArrayDataMap(from: ArrayDataMapTrait, to: String) extends DataMap(to) {
  override def getValue(in: Map[String, Any]): GenericData = {
    new GenericData( from.getValue(in) )
  }
}
