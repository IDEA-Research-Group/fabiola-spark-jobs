package es.us.idea.mapping.internal2.types.basic

import es.us.idea.mapping.Utils

class IntDM(default: Int, value: Option[Int] = None) extends BasicTypeDM[Int](default, value) {
  override def transform(x: Option[Any]): BasicTypeDM[Int] = {
    if(x.isDefined) new IntDM(default, Utils.asInt(x.get))
    else new IntDM(default)
  }
}
