package es.us.idea.mapping.mapper.dsl

import es.us.idea.mapping.mapper.internal.{Helpers, Number, SourcePatterns2}
import org.apache.spark.sql.types.DataTypes
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox
import scala.util.Try

object DSL {

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
    def int(to: String) = FromBasicField(Types.int, to)
    def double(to: String) = FromBasicField(Types.double, to)
    def intArray(to: String) = FromArrayField(Types.intArray, to)
    def doubleArray(to: String) = FromArrayField(Types.doubleArray, to)
    def intMatrix(to: String) = FrommatrixField(Types.intMatrix, to)
    def doubleMatrix(to: String) = FrommatrixField(Types.doubleMatrix, to)
  }


  /**
    * BasicField Case Classes, functions and implicits
    * */
  sealed trait BasicFieldContainerTrait{
    def getFieldObject(): SourcePatterns2.Field // TODO define in each subtype, and call the corresponding function in SourcePatterns

    //def default(str: String): BasicFieldContainer
    def default(number: Number): BasicFieldContainerTrait
    def translate(translations: Translation*): BasicFieldContainerTrait
  }

  case class BasicFieldContainer(path: String, default: Option[Number] = None, translations: Option[Seq[Translation]] = None) extends BasicFieldContainerTrait {
    override def getFieldObject(): SourcePatterns2.Field = new SourcePatterns2.BasicField(path, default, transformTranslations(translations))

    override def default(number: Number) = BasicFieldContainer(path, Option(number))
    override def translate(translations: Translation*): BasicFieldContainer = BasicFieldContainer(path, default, Option(translations))
  }

  case class BasicFieldFromArrayContainer(arrayField: ArrayFieldContainerTrait, reduce: Seq[Number] => Number, default: Option[Number] = None, translations: Option[Seq[Translation]] = None) extends BasicFieldContainerTrait {
    override def getFieldObject(): SourcePatterns2.Field = new SourcePatterns2.BasicFromArrayField(arrayField.getArrayFieldObject(), reduce)

    override def default(number: Number): BasicFieldFromArrayContainer = BasicFieldFromArrayContainer(arrayField, reduce, Option(default))
    def translate(translations: Translation*): BasicFieldFromArrayContainer = BasicFieldFromArrayContainer(arrayField, reduce, Option(default), Option(translations))
  }

  case class Translation(from: Any, to: Option[Number] = None) {
    def ->(number: Number) = Translation(from, Option(number))
  }

  // Utility to transform translation to Map
  def transformTranslations(translations: Option[Seq[Translation]]) =
    Try(translations.get.map(t => (t.from, t.to.get)).toMap).toOption

  implicit def stringToBasicField(str: String): BasicFieldContainerTrait = BasicFieldContainer(str)
  implicit def stringToTranslation(str: String): Translation = Translation(str)
  implicit def intToBasicField(int: Int): BasicFieldContainerTrait = BasicFieldContainer(int.toString)

  //def field[T](str: String): BasicFieldTrait[T] = BasicField[T](str)

  /**
    * ArrayField Case Classes and functions
    * */
  sealed trait ArrayFieldContainerTrait {
    //def default(default: Any) = BasicField(name, Option(default))
    def reduce(f: Seq[Number] => Number): BasicFieldContainerTrait = BasicFieldFromArrayContainer(this, f)
    def getArrayFieldObject(): SourcePatterns2.ArrayField
  }
  case class ArrayFieldContainer(fields: BasicFieldContainerTrait*) extends ArrayFieldContainerTrait {
    override def getArrayFieldObject(): SourcePatterns2.ArrayField = new SourcePatterns2.ArrayFieldFromFields(fields.map(_.getFieldObject()))
  }

  case class ArrayFieldFromPathContainer(path: String, defaultArr: Option[Seq[Number]] = None, default: Option[Number] = None) extends ArrayFieldContainerTrait {
    override def getArrayFieldObject(): SourcePatterns2.ArrayField = new SourcePatterns2.ArrayFieldFromPath(path, defaultArr, default)

    def defaultArray(seq: Seq[Number]) = ArrayFieldFromPathContainer(path, Option(seq), default)
    def defaultValue(number: Number) = ArrayFieldFromPathContainer(path, defaultArr, Option(number))
  }

  case class ArrayFieldFromPathAndFieldsContainer(path: String, fields: ArrayFieldContainer) extends ArrayFieldContainerTrait {
    override def getArrayFieldObject(): SourcePatterns2.ArrayField = new SourcePatterns2.ArrayFieldFromPathAndFields(path, fields.getArrayFieldObject())
  }

  case class ArrayFieldFromMatrixContainer(matrixFieldContainer: MatrixFieldContainer, reduce: Seq[Seq[Number]] => Seq[Number]) extends ArrayFieldContainerTrait {
    override def getArrayFieldObject(): SourcePatterns2.ArrayField = new SourcePatterns2.ArrayFieldFromMatrix(matrixFieldContainer.getMatrixFieldObject(), reduce)
  }

  /**
    * Syntax functions for DSL
    * */
  //def intArray(fields: BasicFieldTrait[Int]*) = ArrayField[Int](fields: _*)
  //def doubleArray(fields: BasicFieldTrait[Double]*) = ArrayField[Double](fields: _*)
  def array(fields: BasicFieldContainerTrait*) = ArrayFieldContainer(fields: _*)
  def array(path: String) = ArrayFieldFromPathContainer(path)
  def array(path: String, fields: ArrayFieldContainer) = ArrayFieldFromPathAndFieldsContainer(path, fields)

  /**
    * MatrixField Case Classes and functions
    */
  sealed trait MatrixFieldContainer {
    def reduce(f: Seq[Seq[Number]] => Seq[Number]): ArrayFieldContainerTrait = ArrayFieldFromMatrixContainer(this, f)
    def getMatrixFieldObject(): SourcePatterns2.MatrixField
  }

  case class MatrixFieldContainerFromPathAndArrayField(path: String, fields: ArrayFieldContainerTrait, rowsByDefault: Option[Int] = None) extends MatrixFieldContainer {
    def rowsByDefault(i: Int) = MatrixFieldContainerFromPathAndArrayField(path, fields, Option(i))
    override def getMatrixFieldObject(): SourcePatterns2.MatrixField = new SourcePatterns2.MatrixFieldFromPathAndArrayField(path, fields.getArrayFieldObject(), rowsByDefault)
  }

  def matrix(path: String, fields: ArrayFieldContainerTrait) = MatrixFieldContainerFromPathAndArrayField(path, fields)

  /**
    * FROM CONTAINER
    * ----------
    *
    * Containers contain information on the data type and the field name, and also on the original data types.
    * (sirven para diferenciar el tipo de field container que reciben
    * */

  // TODO NECESITO TIPOS GENERICOS - a que me referia con esto?
  // Necesitaria otro contenedor. From simplemente debería devolver un objeto de ese nuevo contenedor, y este deberia
  // contener dos metodos: uno que devuelva un Map (nombre del campo, valor) y otro (nombre del campo, tipo de dato)
  // TODO El FROM debería estar presente
  case class FromBasicField(dataType: Type, name: String) {
    def from(field: BasicFieldContainerTrait) = new BasicFieldGetter(dataType, name, field)
  }

  case class FromArrayField(dataType: Type, name: String) {
    def from(field: ArrayFieldContainerTrait) = new ArrayFieldGetter(dataType, name, field)
  }

  case class FrommatrixField(dataType: Type, name: String) {
    def from(field: MatrixFieldContainer) = new MatrixFieldGetter(dataType, name, field)
  }



  // GETTERS
  abstract class FieldGetter(dataType: Type, name: String) {
    def getValue(in: Map[String, Any]) : Any
    def getName() = name
    def getType() = dataType
  }
  case class BasicFieldGetter(dataType: Type, name: String, field: BasicFieldContainerTrait) extends FieldGetter(dataType, name){
    def getValue(in: Map[String, Any]): Any = {
      //field.getFieldObject().get
      dataType match {
        case int: Types.int.type => field.getFieldObject().getNumber(in).getAs[Int]
        case double: Types.double.type => field.getFieldObject().getNumber(in).getAs[Double]
        case _ => None
      }
    }
  }

  case class ArrayFieldGetter(dataType: Type, name: String, field: ArrayFieldContainerTrait)  extends FieldGetter(dataType, name) {
    def getValue(in: Map[String, Any]): Any = {
      //field.getFieldObject().get
      dataType match {
        case intArray: Types.intArray.type => Try(field.getArrayFieldObject().getNumber(in).get.map(x => x.getAs[Int])).toOption
        case doubleArray: Types.doubleArray.type => Try(field.getArrayFieldObject().getNumber(in).get.map(x => x.getAs[Double])).toOption
        case _ => None
      }
    }
  }

  case class MatrixFieldGetter(dataType: DSL.Type, name: String, field: MatrixFieldContainer) extends FieldGetter(dataType, name) {
    override def getValue(in: Map[String, Any]): Any = {
      dataType match {
        case intMatrix: Types.intMatrix.type  => Try(field.getMatrixFieldObject().getNumber(in).get.map(x => if(x.isDefined) x.get.map(_.getAs[Int]))).toOption
        case doubleMatrix: Types.doubleMatrix.type => Try(field.getMatrixFieldObject().getNumber(in).get.map(x => if(x.isDefined) x.get.map(_.getAs[Double]))).toOption
        case _ => None
      }
    }
  }


  def main(args: Array[String]): Unit = {


//    val n = genericNumberTester(14657865456L)
//
//    println(n.getAs[Double].get * 2)
//
//    val s: Seq[Number] = Seq(1, 1.0, 254L, 7897897489456L, 0.0564f, 47)
//
//    println(max(s))

    create double "ID" from("id")
    create int "TAR" from("tarifa" translate("3.0A" -> 0, "3.1A" -> 1))
    create int "MAXEJ" from(array("array") reduce max)
    val c = create double "MAXPOT" from(array("potenciaContratada.p1", "potenciaContratada.p2", "potenciaContratada.p3") reduce max)

    create intArray "EXARR" from(array("array") defaultArray List(90, 91, 92) defaultValue 574)
    create doubleArray "PC" from(array("potenciaContratada.p1", "potenciaContratada.p2", "potenciaContratada.p3"))
    create intArray "EXARRINDEX" from(array("array", array(1, 0)))

    create doubleMatrix "CONS" from(matrix("ejemplo", array(0)))
    create doubleMatrix "POTENCIAS" from(matrix("consumo", array("potencias.p1", "potencias.p2", "potencias.p3")))
    println(c.getValue(SourcePatterns2.inTipo))

    import reflect.runtime.currentMirror

    val code = "import us.es.idea.mapping.mapper.dsl.DSL._\n\n" +
      "create doubleMatrix \"POTENCIAS\" from(matrix(\"consumo\", array(\"potencias.p1\", \"potencias.p2\", \"potencias.p3\")))"
//    val toolbox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
//    val toolBox = currentMirror.mkToolBox()
//
//    // IMPORTS
//    //toolBox.compile(toolBox.parse("import us.es.idea.mapping.mapper.dsl.DSL._"))
//
//    //toolBox.eval(toolBox.parse("println(\"Hola\")"))
//    //toolBox.eval(toolBox.parse(code))
//
//    val obj = toolbox.eval(toolbox.parse(code)).asInstanceOf[FieldGetter]
//    println(obj.getValue(SourcePatterns2.inTipo))

//    val toolbox = currentMirror.mkToolBox()
//    val code1 =
//      q"""
//         import us.es.idea.mapping.mapper.dsl.DSL._
//         object Foo {
//          create doubleMatrix "POTENCIAS" from(matrix("consumo", array("potencias.p1", "potencias.p2", "potencias.p3")))
//         }
//      """
//    println(code1)
//    val result1 = toolbox.compile(code1)
//
//    println(result1)


//    import scala.tools.nsc.Settings
//    import scala.tools.nsc.interpreter._
//    import javax.script._


//    val engine = new ScriptEngineManager().getEngineByName("scala")
//    val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
//    //settings.embeddedDefaults[ToBasicField]
//    //settings.usejavacp.value = true
//    settings.usejavacp.value = true
//
//    engine.eval(""" val x = "TESTA" """)
//    engine.eval(""" println(x) """)
//
//    engine.eval(""" import es.us.idea.mapping.mapper.dsl.DSL._ """)
//    val a = engine.eval(""" create doubleMatrix "POTENCIAS" from(matrix("consumo", array("potencias.p1", "potencias.p2", "potencias.p3"))) """)
//
//    println(a.asInstanceOf[FieldGetter].getValue(SourcePatterns2.inTipo))

    println("Finished")
  }

}
