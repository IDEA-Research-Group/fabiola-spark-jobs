package es.us.idea.mapping.internal.matrix

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal.DataMap
import es.us.idea.mapping.internal.array.ArrayDataMapTrait
import es.us.idea.mapping.internal.types.GenericData

class MatrixDataMap(fromArray: String, subArrays: ArrayDataMapTrait, to: String) extends DataMap(to) {
  override def getValue(in: Map[String, Any]): GenericData = {
    new GenericData( Utils.getValueOfKey(fromArray, in).asInstanceOf[Seq[Any]].map(x => subArrays.getValue(x)))
  }
}
