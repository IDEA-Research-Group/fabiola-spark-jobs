package es.us.idea.mapping

import es.us.idea.mapping.SourcePatterns.{ArrayField, Field, FieldTrait, MatrixField}

//object DSL2 {
//
//  object create {
//    def int(str: String): IntFromContainer = IntFromContainer(str, None, None)
//
//    def double(str: String): Int = 85
//
//    def intArray(str: String): IntArrayFromContainer = {
//      IntArrayFromContainer()
//    }
//
//    def doubleArray(str: String): Int = 85
//
//    def intMatrix(str: String): Int = 85
//
//    def doubleMatrix(str: String): Int = 85
//  }
//
//  //  case class IntFieldContainer(var path: Option[String], var default: Option[Int]) {
//  //    def field(path: String):IntFieldContainer = IntFieldContainer(path = Option(path), None)
//  //  }
//
//  //  case class IntFromContainer(to: String) {
//  //    def from(intFieldContainer: IntFieldContainer): IntFieldContainer = IntFieldContainer(None, None)
//  //  }
//
//  //case class FromContainer()
//
//  case class IntFromContainer(var to: Option[String], var path: Option[String], var default: Option[Int]) {
//    def from /*(intFieldContainer: IntFromContainer)*/ : IntFromContainer = IntFromContainer(to, None, None)
//
//    def field(path: String): IntFromContainer = IntFromContainer(to, Option(path), default)
//
//    def default(default: Int): IntFromContainer = IntFromContainer(to, path, Option(default))
//
//    def arrayField(fromContainer: IntFromContainer*) = IntArrayFromContainer(fromContainer: _*)
//  }
//
//  case class FromContainer(var to: Option[String], var path: Option[String], var default: Option[Any]) {
//    def from /*(intFieldContainer: IntFromContainer)*/ : IntFromContainer = IntFromContainer(to, None, None)
//
//    def field(path: String): IntFromContainer = IntFromContainer(to, Option(path), None) // TODO cambiar
//
//    def default(default: Int): IntFromContainer = IntFromContainer(to, path, Option(default))
//  }
//
//  case class IntArrayFromContainer(FromContainer: IntFromContainer*) {
//
//    def arrayField(fromContainer: IntFromContainer*) = IntArrayFromContainer(fromContainer: _*)
//
//    def field(path: String): IntFromContainer = IntFromContainer(None, Option(path), None)
//
//    def reduce(f: Seq[Int] => Int): IntFromContainer = IntFromContainer(None, None, None) // TODO habria que pasarle la funcion
//    //def arrayField(intFromContainer: IntFromContainer*) = IntArrayFromContainer()
//  }
//
//  case class IntReduceContainer()
//
//  def from = FromContainer(None, None, None)
//
//  def main(args: Array[String]) = {
//
//
//    def max[T](values: Seq[T]): T = values.max
//
//
//    create int "T" field "potenciaContratada.p1" default 0
//    create int "A" arrayField(from field "T" default 5, from field "A" default 0) reduce max
//
//
//    //create intArray "A" arrayField( field "potenciaContratada.p1" default 0, field "potenciaContratada.p1" default 0 )
//
//    //create("T") as double from Field.field("potenciaContratada.p1", 0.0)
//    //create("T") as int from(field("tarifa") default 0)
//    // create("T") as int from( field("tarifa") default 0)
//    // create("T") as int from( arrayField(field("tarifa") default 0, field("otro") default 1), reduce )
//    //println(create("T") as int)
//
//    println("Hi")
//
//
//  }
//}
