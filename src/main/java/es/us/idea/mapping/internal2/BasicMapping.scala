package es.us.idea.mapping.internal2

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal2.types.TypeDM
import es.us.idea.mapping.internal2.types.basic.BasicTypeDM

class BasicMapping[T](from: String, to: String, dmObject: BasicTypeDM[T]) extends Mapping[T](to, dmObject){
  override def getValue(in: Map[String, Any]): BasicTypeDM[T] = {
    dmObject.transform(Utils.getValueOfKey(from, in))
  }
}
