package es.us.idea.mapping.mapper

import es.us.idea.mapping.SourcePatterns
import es.us.idea.mapping.SourcePatterns.FieldTrait

object Mapper {

  // functions
  def max[T](values: Seq[T]): Int = values.map(_.toString.toInt).max
  //def max(values: Seq[Double]): Double = values.max


  sealed trait Type
  object Types {
    case object int extends Type
    case object double extends Type
    case object intArray extends Type
    case object doubleArray extends Type
    case object intMatrix extends Type
    case object doubleMatrix extends Type
  }


  /**
    * MAPPING ENTRY POINT
    * */
  object create {
    def int(to: String) = BasicFieldContainer[Int](Types.int, to)
    def double(to: String) = BasicFieldContainer[Double](Types.double, to)
    def intArray(to: String) = ???
    def doubleArray(to: String) = ???
    def intMatrix(to: String) = ???
    def doubleMatrix(to: String) = ???
  }


  /**
    * BasicField Case Classes, functions and implicits
    * */
  sealed trait BasicFieldTrait[T]{
    //def getValue(): SourcePatterns.Field[T] // TODO define in each subtype, and call the corresponding function in SourcePatterns
  } // should define a method that returns the expected data
  case class BasicField[T](name: String, default: Option[T] = None) extends BasicFieldTrait[T] {
    def default(default: T) = BasicField(name, Option(default))
    //def getValue(): SourcePatterns.Field[T] = SourcePatterns.Field.field[T](name, default.get)
  }
  case class BasicFieldFromArray[T](arrayField: ArrayFieldTrait[T], reduce: Seq[T] => T, default: Option[T] = None) extends BasicFieldTrait[T] {
    //def default(default: Any) = BasicField(name)
    //def getValue(): SourcePatterns.Field[T] = SourcePatterns.Field.field("Foo", default.get)//SourcePatterns.Field.field[T](arrayField.getValue(), reduce)
  }

  implicit def stringToBasicFieldDouble(str: String): BasicFieldTrait[Double] = BasicField[Double](str)
  implicit def stringToBasicFieldInt(str: String): BasicFieldTrait[Int] = BasicField[Int](str)

  def field[T](str: String): BasicFieldTrait[T] = BasicField[T](str)

  /**
    * ArrayField Case Classes and functions
    * */
  sealed trait ArrayFieldTrait[T] {
    //def default(default: Any) = BasicField(name, Option(default))
    def reduce(f: Seq[T] => T): BasicFieldTrait[T] = BasicFieldFromArray[T](this, f)
  }
  case class ArrayFieldFromPath[T](path: String) extends ArrayFieldTrait[T]
  case class ArrayField[T](fields: BasicFieldTrait[T]*) extends ArrayFieldTrait[T]

  //def intArray(fields: BasicFieldTrait[Int]*) = ArrayField[Int](fields: _*)
  //def doubleArray(fields: BasicFieldTrait[Double]*) = ArrayField[Double](fields: _*)
  def array[T](fields: BasicFieldTrait[T]*) = ArrayField[T](fields: _*)
  def array[T](path: String) = ArrayFieldFromPath[T](path)


  /**
    * CONTAINERS
    * ----------
    *
    * Containers contain information on the data type and the field name, and also on the original data types.
    *
    * */

  // Necesitaria otro contenedor. From simplemente debería devolver un objeto de ese nuevo contenedor, y este deberia
  // contener dos metodos: uno que devuelva un Map (nombre del campo, valor) y otro (nombre del campo, tipo de dato)
  case class BasicFieldContainer[T](dataType: Type, name: String) {
    def from(field: BasicFieldTrait[T]) = {
      //Map(name -> field.getValue())
      Map()
    }
  }

  //case class IntFieldContainer(dataType: Type, name: String) {
  //  def from(field: BasicFieldTrait[Int]) = {
  //    Map(name -> field.getValue())
  //  }
  //}


  //def create(str: String) = MapperContainer(str, "aa")

  def main(args: Array[String]): Unit = {
    create int "ID" from("id")
    create double "ID" from("id")
    create int "MAXEJ" from(array("array") reduce max)
    //create int "MAXPOT" from(array(field("potenciaContratada.p1"), field("potenciaContratada.p1")) reduce max)
    //create int "MAXPOT" from(intArray("potenciaContratada.p1", "potenciaContratada.p1") reduce max)

    println("Finished")
  }

}
