package es.us.idea.mapping.mapper.internal

import scala.util.Try

object Helpers {

  /**
    * General purpose helpers
    *
    */
  // Implicit to avoid Option(null)
  implicit class NotNullOption[T](val t: Try[T]) extends AnyVal {
    def toNotNullOption = t.toOption.flatMap{Option(_)}
  }

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
  def transformOpt[T](f:(Option[Any]) => Option[T]) = f
  implicit val transformOptNumber = transformOpt(opt => Number.createNumber(opt.get)) // Transform to Number
  implicit val transformOptSeq = transformOpt(a => Try(asSeq(a.get).get).toOption)
  implicit val transformOptMap = transformOpt(a => Try(asMap(a.get).get).toOption)
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
    * Other type transformations
    */

  implicit class WrapSeqElements[T](val seq: Seq[T]) {
    def wrap() = seq.map(Option(_))
  }

  /**
    * Helpers for generic types.
    *
    * */
  implicit class Pipeline[T](t: Option[T]) {
    def applyPipeline(pipeline: Seq[T => T]) = Try(pipeline.foldLeft(t.get){case (acc, f) => f(acc)}).toOption
  }

  implicit class Reduction[T](st: Option[Seq[Option[T]]]) {
    def applyReduction(reduction: Seq[T] => T): Option[T] = Try(reduction(st.get.map(_.get))).toOption
  }

  implicit class MatrixReduction[T](st: Option[Seq[Option[Seq[Option[T]]]]]) { // TODO Mejorar
    def applyReduction(reduction: Seq[Seq[T]] => Seq[T]): Option[Seq[Option[T]]] = Try(reduction(st.get.map(x => x.get.map(y => y.get))).map(Option(_))).toOption
  }

  // Converters

  def asSeq(value: Any): Option[Seq[Any]] = Try(value.asInstanceOf[Seq[Any]]).toNotNullOption
  def asMap(value: Any): Option[Map[String, Any]] = Try(value.asInstanceOf[Map[String, Any]]).toNotNullOption

}
