package es.us.idea.mapping

//import es.us.idea.mapping.DSL.Types._
//import es.us.idea.mapping.SourcePatterns.{ArrayField, Field, MatrixField}

//object DSL {
//
//  sealed trait Type
//  object Types {
//    case object int extends Type
//    case object double extends Type
//    case object intArray extends Type
//    case object doubleArray extends Type
//    case object intMatrix extends Type
//    case object doubleMatrix extends Type
//  }
//  // from espera recibir un field. Dependiendo del tipo, hace un cast. OJO: TODO ecapsularlo en monads o implicits para evitar petes por conversion de tipos
//  case class ToFieldAndType(toField: String, value: DSL.Type) {
//    // TODO aquí se consultaría el tipo y se iría almacenando las variables en sus respectivos maps
//    def from(f: FieldTrait) = {
//      import Helpers._
//      println(value)
//      value match {
//        case i: int.type => println("AAA667"); f.asInstanceOf[Field[Int]]
//        case d: double.type =>  println("AAA"); f.asInstanceOf[Field[Double]]; println(f.asInstanceOf[Field[Double]].getValue(SourcePatterns.inTipo)); println("aaa")
//        case ia: intArray.type => f.asInstanceOf[ArrayField[Int]]
//        case da: doubleArray.type => f.asInstanceOf[ArrayField[Double]]
//        case im: intMatrix.type => f.asInstanceOf[MatrixField[Int]]
//        case dm: doubleMatrix.type => f.asInstanceOf[MatrixField[Double]]
//        case _ => println("didnt match")
//      }
//    }
//  }
//
//  case class ToField(str: String) {
//    def as(t: Type): ToFieldAndType = ToFieldAndType(str, t)
//  }
//
//  def create(to: String): ToField = {
//    ToField(to)
//  }
//
//
//  // DSL for field
//
//
//  //case class FieldPath(str: String) {
//  //  def default(d: )
//  //}
//
//  //def field(path: String): FieldPath = {
//  //  FieldPath(path)
//  //}
//
//
//  def main(args:Array[String]) = {
//
//
//    create("T") as double from Field.field("potenciaContratada.p1", 0.0)
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
