package es.us.idea.mapping.internal2

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal2.types.TypeDM
import es.us.idea.mapping.internal2.types.array.ArrayTypeDM

class ArrayMapping[T](from: String, to: String, dmObject: ArrayTypeDM[T]) extends Mapping[T](to, dmObject) {
  override def getValue(in: Map[String, Any]): ArrayTypeDM[T] = {
    dmObject.transform(Utils.getValueOfKey(from, in))
  }
}
