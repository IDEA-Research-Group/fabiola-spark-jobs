package es.us.idea.mapping.mapper.dsl

import es.us.idea.mapping.mapper.internal.Number

import scala.util.Try

object DSL {

  /**
    * NUMERIC TYPE LOGIC
    * it shouk
    * */
// Number wrapper class
//class Number(n: Double) {
//  def this(s: String) = this(s.toDouble)
//  // Seems like the conversion to Double of these types is automatic
//  //def this(i: Int) = this(i.toDouble)
//  //def this(f: Float) = this(f.toDouble)
//  //def this(l: Long) = this(l.toDouble)
//  def getDouble = n
//  override def toString = n.toString
//}

//  implicit def ordering[A <: Number]: Ordering[A] = Ordering.by(_.getDouble)
//
//  // These implicits allow to transform a Number to Int, Double, Float or Long by calling .getAs[T]
//  def transform[T](f:(Number) => Option[T]) = f
//  implicit val transformString = transform(a => Try(a.getDouble.toString).toOption)
//  implicit val transformInt = transform(a => Try(a.getDouble.toInt).toOption)
//  implicit val transformDouble = transform(a => Try(a.getDouble).toOption)
//  implicit val transformFloat = transform(a => Try(a.getDouble.toFloat).toOption)
//  implicit val transformLong = transform(a => Try(a.getDouble.toLong).toOption)
//
//  implicit class NumberTransform(a: Number) {
//    def getAs[T](implicit run: Number => Option[T]): Option[T] = run(a)
//  }
//
//  // Implicits to transform Int, Double, Float and Logs to Number
//  implicit def stringToNumber(i: String) = new Number(i)
//  implicit def intToNumber(i: Int) = new Number(i)
//  implicit def doubleToNumber(d: Double) = new Number(d)
//  implicit def floatToNumber(f: Float) = new Number(f)
//  implicit def longToNumber(l: Long) = new Number(l)
//
//  // Just for testing purposes
//  def genericNumberTesterPrint(x: Number) = println(s"success $x")
//  def genericNumberTester(x: Number) = x

  /**
    * DSL LOGIC
    *
    * */

  // functions
  def max(values: Seq[Number])(implicit ordering: Ordering[Number]) = values.max
  //def max(values: Seq[Double]): Double = values.max


  sealed trait Type
  object Types {
    case object int extends Type
    case object double extends Type
    case object intArray extends Type
    case object doubleArray extends Type
    case object intMatrix extends Type
    case object doubleMatrix extends Type
    case object string extends Type
  }


  /**
    * MAPPING ENTRY POINT
    * */
  object create {
    def int(to: String) = BasicFieldContainer(Types.int, to)
    def double(to: String) = BasicFieldContainer(Types.double, to)
    def intArray(to: String) = ???
    def doubleArray(to: String) = ???
    def intMatrix(to: String) = ???
    def doubleMatrix(to: String) = ???
  }


  /**
    * BasicField Case Classes, functions and implicits
    * */
  sealed trait BasicFieldTrait{
    //def getValue(): SourcePatterns.Field[T] // TODO define in each subtype, and call the corresponding function in SourcePatterns
  } // should define a method that returns the expected data
  case class BasicField(name: String, default: Option[Number] = None) extends BasicFieldTrait {
    def default(default: Number) = BasicField(name, Option(default))
    //def getValue(): SourcePatterns.Field[T] = SourcePatterns.Field.field[T](name, default.get)
  }
  case class BasicFieldFromArray(arrayField: ArrayFieldTrait, reduce: Seq[Number] => Number, default: Option[Number] = None) extends BasicFieldTrait {
    //def default(default: Any) = BasicField(name)
    //def getValue(): SourcePatterns.Field[T] = SourcePatterns.Field.field("Foo", default.get)//SourcePatterns.Field.field[T](arrayField.getValue(), reduce)
  }

  implicit def stringToBasicField(str: String): BasicFieldTrait = BasicField(str)

  //def field[T](str: String): BasicFieldTrait[T] = BasicField[T](str)

  /**
    * ArrayField Case Classes and functions
    * */
  sealed trait ArrayFieldTrait {
    //def default(default: Any) = BasicField(name, Option(default))
    def reduce(f: Seq[Number] => Number): BasicFieldTrait = BasicFieldFromArray(this, f)
  }
  case class ArrayFieldFromPath(path: String) extends ArrayFieldTrait
  case class ArrayField(fields: BasicFieldTrait*) extends ArrayFieldTrait

  //def intArray(fields: BasicFieldTrait[Int]*) = ArrayField[Int](fields: _*)
  //def doubleArray(fields: BasicFieldTrait[Double]*) = ArrayField[Double](fields: _*)
  def array(fields: BasicFieldTrait*) = ArrayField(fields: _*)
  def array(path: String) = ArrayFieldFromPath(path)


  /**
    * CONTAINERS
    * ----------
    *
    * Containers contain information on the data type and the field name, and also on the original data types.
    *
    * */

  // Necesitaria otro contenedor. From simplemente debería devolver un objeto de ese nuevo contenedor, y este deberia
  // contener dos metodos: uno que devuelva un Map (nombre del campo, valor) y otro (nombre del campo, tipo de dato)
  case class BasicFieldContainer(dataType: Type, name: String) {
    def from(field: BasicFieldTrait) = {
      //Map(name -> field.getValue())
      Map()
    }
  }

  def main(args: Array[String]): Unit = {

    import es.us.idea.mapping.mapper.internal.Number._

    val n = genericNumberTester(14657865456L)

    println(n.getAs[Double].get * 2)

    val s: Seq[Number] = Seq(1, 1.0, 254L, 7897897489456L, 0.0564f, 47)

    println(max(s))

    create int "ID" from("id")
    create double "ID" from("id")
    create int "MAXEJ" from(array("array") reduce max)
    create int "MAXPOT" from(array("potenciaContratada.p1", "potenciaContratada.p1") reduce max)
    //create int "MAXPOT" from(intArray("potenciaContratada.p1", "potenciaContratada.p1") reduce max)


    val either: Either[Number, String] = Left(null)

    println(either.left)


    println("Finished")
  }

}
