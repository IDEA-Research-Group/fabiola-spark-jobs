package es.us.idea.mapping


import scala.util.Try

object Helpers {
  // Implicit to avoid Option(null)
  implicit class NotNullOption[T](val t: Try[T]) extends AnyVal {
    def toNotNullOption = t.toOption.flatMap{Option(_)}
  }

  // Implicit to directly get the value of an Option[Map] if Option is defined AND the key exists or otherwise return default val
  implicit class GetKeyOrElse(val om: Option[Map[Any, Any]]) {
    def getKeyOrElse(key: Any, default: Any) = if (om.isDefined) om.get.getOrElse(key, default) else default
  }

  implicit class GetValueFromPath(val map: Map[String, Any]) {
    def getValueFromPath(path: String) = recursiveGetValueFromPath(path, map)
    def getValueFromPathOrElse(path: String, default: Any) = getValueFromPath(path).getOrElse(default)
  }

  // Implicit declarations
  //object Transform {
  def transform[T](f:(Any) => Option[T]) = f
  implicit val transformInt = transform(a => Try(asInt(a).get).toOption)
  implicit val transformDouble = transform(a => Try(asDouble(a).get).toOption)
  implicit val transformSeq = transform(a => Try(asSeq(a).get).toOption)
  implicit val transformMap = transform(a => Try(asMap(a).get).toOption)
  //}

  implicit class AnyType(a: Any) {
    def getAs[T](t:  Option[Any => Any] = None)(implicit run: Any => Option[T]): Option[T] = if(t.isDefined) run(t.get(a)) else run(a)
    def getAsOrElse[T](default: T, t: Option[Any => Any] = None)(implicit run: Any => Option[T]) = getAs[T](t).getOrElse(default)
  }

  implicit class GenericType[T](t: T) {
    def applyPipeline(pipeline: Seq[T => T]) = pipeline.foldLeft(t){case (acc, f) => f(acc)}
  }

  implicit class SeqOfGenericType[T](st: Seq[T]) {
    def applyReduction(reduction: Seq[T] => T) = reduction(st)
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

  // Conversions
  def asDouble(value: Any): Option[Double] = {
    value match {
      case s: String => Try(s.toDouble).toNotNullOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }
  def asInt(value: Any): Option[Int] = Try(asDouble(value).get.toInt).toNotNullOption
  def asSeq(value: Any): Option[Seq[Any]] = Try(value.asInstanceOf[Seq[Any]]).toNotNullOption
  def asMap(value: Any): Option[Map[String, Any]] = Try(value.asInstanceOf[Map[String, Any]]).toNotNullOption

}
