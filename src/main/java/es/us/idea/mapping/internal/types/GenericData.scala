package es.us.idea.mapping.internal.types

import scala.util.Try

//sealed trait Base[T <: Base[T]]

class GenericData(value: Option[Any]) /*extends Base[T]*/ {

  //def transform(default: T): T = {
//
  //  if(value.isDefined) {
  //    T match {
  //      case i => Double: asDouble(value.get)
  //    }
  //  } else default
  //}

  def asDouble(): Option[Double] = {
    value.get match {
      case s: String => Try(s.toDouble).toOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }
  def asInt(): Option[Int] = Try(asDouble().get.toInt).toOption
  def asString(): Option[String] = Try(value.asInstanceOf[String]).toOption

  // def asDoubleMatrix(): Seq[Option[Seq[Option[Double]]]] = {
  //   Try(value.get.asInstanceOf[ Seq[Option[Seq[Option[Double]]]] ]).getOrElse(Seq(Option(Seq())))
  // }

  def asDoubleArray(): Option[Seq[Option[Any]]] = {
    Try(value.asInstanceOf[ Option[Seq[Option[Any]]] ]).getOrElse(Option(Seq()))
  }

  def asDoubleMatrix(): Option[Seq[Option[Seq[Option[Double]]]]] = {
    Try(value.asInstanceOf[ Option[Seq[Option[Seq[Option[Double]]]]] ]).getOrElse(Option(Seq(Option(Seq()))))
  }
  def asIntMatrix(): Option[Seq[Option[Seq[Option[Int]]]]] = {
    Try(value.asInstanceOf[ Option[Seq[Option[Seq[Option[Int]]]]] ]).getOrElse(Option(Seq(Option(Seq()))))
  }

}

object GenericData {
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
}