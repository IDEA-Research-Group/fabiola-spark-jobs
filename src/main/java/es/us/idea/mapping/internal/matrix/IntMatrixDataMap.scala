package es.us.idea.mapping.internal.matrix

import es.us.idea.mapping.internal.array.collaborators.ArrayDataMapTrait

class IntMatrixDataMap(fromArray: String, subArrays: Option[ArrayDataMapTrait], to: String) extends MatrixDataMap(fromArray, subArrays, to) {

  /*
  * Comprobar si algun componente de la matriz es nula, sustituirlo por:
  * - 0
  * - Lista vacía
  * - Matríz vacía
  * */
  def get(in: Map[String, Any]): Seq[Seq[Int]] = {
    val optMatrix = super.getValue(in).asIntMatrix()

    println(s"Interno: $optMatrix")

    if (optMatrix.isDefined)
      optMatrix.get.map( x => if(x.isDefined) x.get.map(y => if(y.isDefined) y.get else 0 ) else Seq())
    else Seq(Seq())
  }

}
