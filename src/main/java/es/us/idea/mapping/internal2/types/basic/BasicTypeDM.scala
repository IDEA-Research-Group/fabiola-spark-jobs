package es.us.idea.mapping.internal2.types.basic

import es.us.idea.mapping.internal2.types.TypeDM

abstract class BasicTypeDM[T](default: T, value: Option[T] = None) extends TypeDM[T] {
  def transform(x: Option[Any]): BasicTypeDM[T]
  def get(): T = value.getOrElse(default)
}
