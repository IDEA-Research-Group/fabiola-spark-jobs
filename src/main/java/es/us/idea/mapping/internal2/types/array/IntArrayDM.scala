package es.us.idea.mapping.internal2.types.array

import es.us.idea.mapping.Utils

import scala.util.Try

class IntArrayDM(defaultArr: Seq[Int], defaultVal: Int, value: Option[Seq[Option[Int]]] = None) extends ArrayTypeDM[Int](defaultArr, defaultVal, value){
  // x is expected to be a Seq of Any
  override def transform(x: Option[Any]): IntArrayDM = new IntArrayDM(defaultArr, defaultVal, Try(x.get.asInstanceOf[Seq[Any]].map(x => Utils.asInt(x))).toOption)

}
