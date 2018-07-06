package es.us.idea.mapping.internal.matrix

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal.DataMap
import es.us.idea.mapping.internal.array.collaborators.ArrayDataMapTrait
import es.us.idea.mapping.internal.types.GenericData

import scala.util.Try

class MatrixDataMap(fromArray: String, subArrays: Option[ArrayDataMapTrait], to: String) extends DataMap(to) {

  override def getValue(in: Map[String, Any]): GenericData = {
    // If subArrays is not defined, it means that the map should be directly carried out
    if(subArrays.isEmpty) new GenericData( Try(Utils.getValueOfKey(fromArray, in).get.asInstanceOf[Seq[Any]].map(_.asInstanceOf[Seq[Any]])).toOption )
    else new GenericData( Option(Utils.getValueOfKey(fromArray, in).get.asInstanceOf[Seq[Any]].map(x => subArrays.get.getValue(x))) )
  }
}
