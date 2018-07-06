package es.us.idea.mapping.internal2.types.basic

import es.us.idea.mapping.Utils

class DoubleDM(default: Double, value: Option[Double] = None) extends BasicTypeDM[Double](default, value) {
  override def transform(x: Option[Any]): BasicTypeDM[Double] = {
    if(x.isDefined) new DoubleDM(default, Utils.asDouble(x.get))
    else new DoubleDM(default)
  }
}
