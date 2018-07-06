package es.us.idea.mapping.internal2.types

trait TypeDM[T] {
  def transform(x: Option[Any]): TypeDM[T]
  //def get(): T
}
