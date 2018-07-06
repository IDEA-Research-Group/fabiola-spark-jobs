package es.us.idea.mapping

import scala.util.Try

object Functions2 {
//  object Parse {
//    def parse[T](f:String => Option[T]) = f
//    implicit val parseInt = parse(s => Try(s.toInt).toOption)
//    implicit val parseLong = parse(s => Try(s.toLong).toOption)
//    implicit val parseDouble = parse(s => Try(s.toDouble).toOption)
//    implicit val parseBoolean = parse(s => Try(s.toBoolean).toOption)
//  }
//
//  implicit class MyString(s:String) {
//    def getAs[T]()(implicit run: String => Option[T]): Option[T] = run(s)
//  }
//
////  def getValue[T](a: String): T = {
////    import Parse._
////    a.getAs[T]().get
////  }
//
//  def main(args: Array[String]) {
//    import Parse._
//    "true".getAs[Boolean].foreach(println)
//    "12345".getAs[Int].foreach(println)
////    println(getValue[Double]("1.0"))
//  }


  def convertAny[T](any: Any)(implicit run: Any => Option[T]) = run.apply(any)
  implicit def anyToDouble(any: Any) = Try(any.asInstanceOf[Double]).toOption
  implicit def anyToInt(any: Any) = Try(any.asInstanceOf[Int]).toOption


  def doStuffAndConvert[T](i: Any)(implicit run: Any => Option[T]): Option[T] = {
    // Some pre-processing
    println("Processing data...")

    convertAny[T](i)
  }


  def main(args: Array[String]) = {

    val a = 5.0

    println(convertAny[Double](a))
    println(doStuffAndConvert[Double](a))

  }


}
