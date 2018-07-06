package es.us.idea.mapping.internal2

import es.us.idea.mapping.internal2.types.TypeDM

abstract class Mapping[T](to: String, dmObject: TypeDM[T]) {
  def getValue(in: Map[String, Any]): TypeDM[T]
}
