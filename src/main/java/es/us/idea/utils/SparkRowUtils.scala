package es.us.idea.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable

/*****************************
  * This Scala Obejct contains methods that allow to transform
  * a Spark Row object into a Map[String, Any]
  ***************************/
object SparkRowUtils {

  def fromRowToMap(row: Row):Map[String, Any] = {
    format(row.schema, row)
  }

  private def format(schema: Seq[StructField], row: Row): Map[String, Any] = {
    var result:Map[String, Any] = Map()

    schema.foreach(s => //println(s.dataType)
      s.dataType match {
        case ArrayType(elementType, _)=> val thisRow = row.getAs[mutable.WrappedArray[Any]](s.name); result = result ++ Map(s.name -> formatArray(elementType, thisRow))
        case StructType(structFields)=> val thisRow = row.getAs[Row](s.name); result = result ++ Map( s.name -> format(thisRow.schema, thisRow))
        case _ => result = result ++ Map(s.name -> row.getAs(s.name))
      }
    )
    return result
  }

  private def formatArray(elementType: DataType, array: mutable.WrappedArray[Any]): Seq[Any] = {
    elementType match {
      case StructType(structFields) => array.map(e => format(structFields, e.asInstanceOf[Row]))
      case ArrayType(elementType2, _) => array.map(e => formatArray(elementType2, e.asInstanceOf[mutable.WrappedArray[Any]]))
      case _ => array
    }
  }
}
