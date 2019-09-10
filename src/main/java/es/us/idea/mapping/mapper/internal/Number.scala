package es.us.idea.mapping.mapper.internal

import scala.util.Try

class Number(n: Double) {

  def this(s: String) = this(s.toDouble)
  def this(a: Any) = this(a.toString.toDouble)

  // Seems like the conversion to Double of these types is automatic
  //def this(i: Int) = this(i.toDouble)
  //def this(f: Float) = this(f.toDouble)
  //def this(l: Long) = this(l.toDouble)

  def getDouble = n

  override def toString = n.toString
}

object Number {

  def createNumber(a: Any) = Try(new Number(a)).toOption

  implicit def ordering[A <: Number]: Ordering[A] = Ordering.by(_.getDouble)

  // These implicits allow to transform a Number to Int, Double, Float or Long by calling .getAs[T]
  def transform[T](f:(Number) => Option[T]) = f
  implicit val transformString = transform(a => Try(a.getDouble.toString).toOption)
  implicit val transformInt = transform(a => Try(a.getDouble.toInt).toOption)
  implicit val transformDouble = transform(a => Try(a.getDouble).toOption)
  implicit val transformFloat = transform(a => Try(a.getDouble.toFloat).toOption)
  implicit val transformLong = transform(a => Try(a.getDouble.toLong).toOption)

  implicit class NumberTransform(a: Number) {
    def getAs[T](implicit run: Number => Option[T]): Option[T] = run(a)
  }

  implicit class OptionalNumberTransform(a: Option[Number]) {
    def getAs[T](implicit run: Number => Option[T]): Option[T] = if(a.isDefined) run(a.get) else None
  }

  // Implicits to transform Int, Double, Float and Logs to Number
  implicit def stringToNumber(i: String) = new Number(i)
  implicit def anyToNumber(a: Any) = new Number(a)
  implicit def intToNumber(i: Int) = new Number(i)
  implicit def doubleToNumber(d: Double) = new Number(d)
  implicit def floatToNumber(f: Float) = new Number(f)
  implicit def longToNumber(l: Long) = new Number(l)

  // Just for testing purposes
  def genericNumberTesterPrint(x: Number) = println(s"success $x")
  def genericNumberTester(x: Number) = x

}