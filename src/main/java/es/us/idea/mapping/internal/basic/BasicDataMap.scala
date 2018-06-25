package es.us.idea.mapping.internal.basic

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal.DataMap
import es.us.idea.mapping.internal.types.GenericData

class BasicDataMap(from: String, to: String) extends DataMap(to){
  override def getValue(in: Map[String, Any]): GenericData = new GenericData(Utils.getValueOfKey(from, in))
}
