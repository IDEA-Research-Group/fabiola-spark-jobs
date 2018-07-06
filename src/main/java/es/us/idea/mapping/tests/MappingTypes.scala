package es.us.idea.mapping.tests

import es.us.idea.mapping.Utils


sealed trait MappingType[T <: MappingType[T]] {
  def get(): T
}

sealed trait GenericData[T] {
  def get(): T
}

class IntGenericData(value: Any) extends GenericData[Int] {
  override def get(): Int = {
    0
  }
}



//class GenericData[T] (value: Any) {
//
//  def get() : T = {
//    val t = value.asInstanceOf[T]
//
//    t match {
//      case d: Double => Utils.
//    }
//
//
//  }
//
//}


object MappingTypes {

}
