package es.us.idea.mapping.mapper.internal

import scala.util.Try
import Number._

object Helpers {

  /**
    * HELPERS FOR Map[String, Any]
    *
    * */

  // TODO revisar utilidad
  // Implicit to directly get the value of an Option[Map] if Option is defined AND the key exists or otherwise return default val
  //implicit class GetKeyOrElse(val om: Option[Map[Any, Any]]) {
  //  def getKeyOrElse(key: Any, default: Any) = if (om.isDefined) om.get.getOrElse(key, default) else default
  //}


  implicit class GetValueFromPath(val map: Map[String, Any]) {
    def getValueFromPath(path: String, default: Option[Any] = None): Option[Any] = {
      val value = recursiveGetValueFromPath(path, map)
      if(value.isDefined) value else default
    }
    //def getValueFromPathOrElse(path: String, default: Any) = getValueFromPath(path).getOrElse(default)
  }

  def recursiveGetValueFromPath(path: String, map: Map[String, Any]): Option[Any] = {
    if (path.contains(".")) {
      val keySplt = path.split('.')
      if (map.contains(keySplt.head)) {
        return recursiveGetValueFromPath(keySplt.tail.mkString("."), map(keySplt.head).asInstanceOf[Map[String, Any]])
      } else {
        return None
      }
    } else {
      return map.get(path) // .getOrElse(null)
    }
  }

  /**
    * HELPERS FOR Option[Any]
    *
    * */

  def transform[T](f:(Option[Any]) => Option[T]) = f
  implicit val transformNumber = transform(opt => Try(createNumber(opt.get)).toOption) // Transform to Number
  //implicit val transformString = transform(opt => Try(opt.get.toString).toOption)      // Transform to String


  //implicit val transformInt = transform(a => Try(asInt(a).get).toOption)
  //implicit val transformDouble = transform(a => Try(asDouble(a).get).toOption)
  //implicit val transformSeq = transform(a => Try(asSeq(a).get).toOption)
  //implicit val transformMap = transform(a => Try(asMap(a).get).toOption)

  implicit class GetTypedOption(val opt: Option[Any]) {
    def getAs[T](translations:  Option[Any => Any] = None)(implicit run: Option[Any] => Option[T]): Option[T] =
      if(translations.isDefined) run(Try(translations.get(opt.get)).toOption) else run(opt)
    //def getAsOrElse[T](default: T, translations: Option[Any => Any] = None)(implicit run: Any => Option[T]) = getAs[T](translations).getOrElse(default)

  }


  /**
    * Helpers for generic types.
    *
    * */
  implicit class GenericType[T](t: Option[T]) {
    def applyPipeline(pipeline: Seq[T => T]) = Try(pipeline.foldLeft(t.get){case (acc, f) => f(acc)}).toOption
  }


}
