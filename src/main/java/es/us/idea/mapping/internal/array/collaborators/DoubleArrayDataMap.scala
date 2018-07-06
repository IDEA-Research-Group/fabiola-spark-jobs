package es.us.idea.mapping.internal.array.collaborators

import es.us.idea.mapping.internal.array.ArrayDataMap

class DoubleArrayDataMap(from: ArrayDataMapTrait, to: String) extends ArrayDataMap(from, to) {

  //def get(in: Map[String, Any]): Seq[Double] = {
  //  val optArray = super.getValue(in).asDoubleArray()
  //  if (optArray.isDefined)
  //    optArray.get.map( x => if(x.isDefined) x.get.map(y => if(y.isDefined) y.get else 0.0 ) else Seq())
  //  else Seq(Seq())
  //}

}
