package es.us.idea.mapping.internal2.types.array

import es.us.idea.mapping.internal2.types.TypeDM

abstract class ArrayTypeDM[T](defaultArr: Seq[T], defaultVal: T, value: Option[Seq[Option[T]]] = None) extends TypeDM[T]{
  override def transform(x: Option[Any]): ArrayTypeDM[T]

  def get(): Seq[T] = if(value.isDefined) value.get.map(_.getOrElse(defaultVal)) else defaultArr

}
