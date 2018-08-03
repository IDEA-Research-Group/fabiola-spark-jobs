package es.us.idea.mapping

import es.us.idea.utils.DMSelect
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.util.Try

object Utils {

  def buildRowFromMapAndDMSelector(in: Map[String, Any], dmSelector: Seq[DMSelect]) = {
    Row.apply(dmSelector.map(x => in.get(x.name)): _*)

  }

  def generateDataTypeFromDMSelector(dmSelector: Seq[DMSelect]) = {
    DataTypes.createStructType(dmSelector.map( x => DataTypes.createStructField(x.name, strTypeToDataType(x.dataType), true)).toArray)
  }

  def strTypeToDataType(str: String) = {
    str match {
      case "Int" => DataTypes.IntegerType
      case "Double" => DataTypes.DoubleType
      case "String" => DataTypes.StringType
      case "IntArray" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "DoubleArray" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "IntMatrix" => DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType))
      case "DoubleMatrix" => DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType))
      case _ => DataTypes.NullType
    }
  }

  def generateDataType(in: Map[String, Any]) = {
    DataTypes.createStructType(in.map(x => DataTypes.createStructField(x._1, anyToDataType(x._2), true)).toArray)
  }

  def anyToDataType(any: Any): DataType = {
    any match {
      case any: Int => DataTypes.IntegerType
      case any: Double => DataTypes.DoubleType
      case any: String => DataTypes.StringType
      case _ => DataTypes.NullType
    }
  }


  def getValueOfKey(key: String, map: Map[String, Any]): Option[Any] = {
    if (key.contains(".")) {
      val keySplt = key.split('.')
      if (map.contains(keySplt.head)) {
        return getValueOfKey(keySplt.tail.mkString("."), map.get(keySplt.head).get.asInstanceOf[Map[String, Any]])
      } else {
        return null
      }
    } else {
      return map.get(key) // .getOrElse(null)
    }
  }

  def asDouble(value: Any): Option[Double] = {
    value match {
      case s: String => Try(s.toDouble).toOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }

  def asInt(value: Any): Option[Int] = Try(asDouble(value).get.toInt).toOption

}
