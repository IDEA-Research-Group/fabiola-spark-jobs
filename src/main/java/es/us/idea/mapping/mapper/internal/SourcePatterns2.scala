package es.us.idea.mapping.mapper.internal

import Helpers._

object SourcePatterns2 {

  val inTipo = Map(
    "id" -> 500,
    "array" -> Seq(0.0, 1.0, null, 80, null),
    "ejemplo" -> Seq(Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(null, 1.0), null),
    //"ejemplo" -> Seq(),
    //"ejemplo" -> Seq(Seq()),
    "ICPInstalado" -> "Icp no instalado", "derechosExtension" -> 32.91, "tension" -> "3X220/380V", "propiedadEqMedidaTitular" -> "Empresa distribuidora",
    "potenciaContratada" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 32.91, "p2" -> 32.91, "p1" -> 32.91, "p6" -> 0.0), "impagos" -> "NO", "tipoFrontera" -> 4,
    "tarifa" -> "3.0A", "ubicacionPoblacion" -> "SOMO", "potMaxBie" -> 32.91, "distribuidora" -> "0027 - VIESGO DISTRIBUCION ELECTRICA, S.L.", "fechaAltaSuministro" -> "24/04/1991", "DH" -> "DH3", "totalFacturaActual" -> 4098.68,
    "propiedadICPTitular" -> "Empresa distribuidora", "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria",
    "consumo" -> Seq(
      Map("arrayAnidado" -> Seq(1.0, 5.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014"),
      Map("arrayAnidado" -> Seq(2.0, 10.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 10.0, "p2" -> 20.0, "p1" -> 20.0, "p6" -> 0.0), "anio" -> 2015, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")
    ),
    "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> 32.91)

  //trait FieldTrait

  trait Field {
    def getValue(in: Map[String, Any])/*(implicit ev: Option[Any] => Option[Number])*/: Option[Number]
    // TODO getString & getNumber???
    def getDefault(): Number
  }

  trait ArrayField {
    def getValue(in: Map[String, Any])(implicit ev: Option[Any] => Option[Number]): Seq[Number]
    def getDefault(): Seq[Number]
  }

  trait MatrixField {
    def getValue(in: Map[String, Any])(implicit ev: Option[Any] => Option[Number]): Seq[Seq[Number]]
    def getDefault(): Seq[Seq[Number]]
  }

  // FIELD
  class BasicField(path: String, default: Option[Number]=None, translations: Option[Any => Number] = None, transformations: Option[Seq[Number => Number]] = None) extends Field {
    override def getValue(in: Map[String, Any])/*(implicit ev: Option[Any] => Option[Number])*/: Option[Number] =
      in.getValueFromPath(path).getAs[Number](translations).applyPipeline(transformations.getOrElse(Seq()))
    override def getDefault(): Number = default
  }
/*
  class BasicFromArrayField[T](arrayField: ArrayField[T], reduction: Seq[T] => T) extends Field[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): T =
      arrayField.getValue(in).applyReduction(reduction) // Apply pipeline
    override def getDefault(): T = arrayField.getDefault().applyReduction(reduction)
  }

  object Field {
    def field[T](path: String, default: T): Field[T] = new BasicField[T](path, default)
    def field[T](path: String, default: T, translations: Any => Any): Field[T] = new BasicField[T](path, default, Option(translations))
    def field[T](path: String, default: T, transformations: Seq[T => T]): Field[T] = new BasicField[T](path, default, None, Option(transformations))
    def field[T](path: String, default: T, translations: Any => Any, transformations: Seq[T => T]): Field[T] = new BasicField[T](path, default, Option(translations), Option(transformations))

    def field[T](arrayField: ArrayField[T], reduction: Seq[T] => T): Field[T] = new BasicFromArrayField[T](arrayField, reduction)
  }





  // ARRAY FIELD
  class ArrayFieldFromFields[T](fields: Seq[Field[T]]) extends ArrayField[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[T] =
      fields.map(_.getValue(in))
    override def getDefault(): Seq[T] = fields.map(_.getDefault())
  }

  class ArrayFieldFromPath[T](path: String, defaultArr: Seq[T], default: T) extends ArrayField[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[T] =
      in.getValueFromPathOrElse(path, defaultArr).getAsOrElse[Seq[Any]](defaultArr).zipWithIndex.map{case (e, i) => e.getAsOrElse[T](defaultArr.lift(i).getOrElse(default))}
    override def getDefault(): Seq[T] = defaultArr
  }

  // path is expected to be an array of basic types and fields the indexes of that array
  class ArrayFieldFromPathAndFields[T](path: String, fields: ArrayField[T]) extends ArrayField[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[T] = {
      val defaultArr = fields.getDefault()
      fields.getValue(
        in
          .getValueFromPathOrElse(path, defaultArr)
          .getAsOrElse[Seq[Any]](defaultArr)
          .zipWithIndex
          .map(el => (el._2.toString, el._1))
          .toMap
      )
    }
    override def getDefault(): Seq[T] = fields.getDefault()
  }

  class ArrayFieldFromMatrix[T](matrixField: MatrixField[T], reduction: Seq[Seq[T]] => Seq[T]) extends ArrayField[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[T] = matrixField.getValue(in).applyReduction(reduction)
    override def getDefault(): Seq[T] = matrixField.getDefault().applyReduction(reduction)
  }


  object ArrayField {
    def arrayField[T](fields: Seq[Field[T]]): ArrayField[T] = new ArrayFieldFromFields(fields)

    def arrayField[T](path: String, defaultArr: Seq[T], default: T): ArrayField[T] = new ArrayFieldFromPath[T](path, defaultArr, default)

    def arrayField[T](path: String, fields: ArrayField[T]): ArrayField[T] = new ArrayFieldFromPathAndFields[T](path, fields)

    def arrayField[T](matrixField: MatrixField[T], reduction: Seq[Seq[T]] => Seq[T]) = new ArrayFieldFromMatrix[T](matrixField, reduction)
  }


  // MATRIX FIELD

  class MatrixFieldFromArrayFields[T](arrayFields: Seq[ArrayField[T]]) extends MatrixField[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[Seq[T]] =
      arrayFields.map(_.getValue(in))
    override def getDefault(): Seq[Seq[T]] = arrayFields.map(_.getDefault())
  }

  class MatrixFieldFromPathAndArrayField[T](pathToArray: String, arrayField: ArrayField[T], defaultNumberOfRows: Int) extends MatrixField[T] {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[Seq[T]] = {
      val seq = in.getValueFromPathOrElse(pathToArray, getDefault()).getAs[Seq[Any]]()

      // Para cada caso, ha de diferenciar si cada elemento de la secuencia (Any) es otra secuencia o un map
      if(seq.isDefined) {
        seq.get.map(x => {
          if(x.isInstanceOf[Seq[Any]]) {
            // Tratar como un array
            arrayField.getValue(
              x.getAsOrElse[Seq[Any]](arrayField.getDefault())
                .zipWithIndex
                .map(el => (el._2.toString, el._1))
                .toMap
            )
          } else {
            // Tratar como una estructura
            val map = x.getAs[Map[String, Any]]()
            if(map.isDefined) {
              arrayField.getValue(map.get)
            } else arrayField.getDefault()
          }
        })
      } else getDefault()
    }

    override def getDefault(): Seq[Seq[T]] = Seq.fill[Seq[T]](defaultNumberOfRows)(arrayField.getDefault())
  }

  object MatrixField {
    def matrixField[T](arrayFields: Seq[ArrayField[T]]): MatrixField[T] = new MatrixFieldFromArrayFields[T](arrayFields)

    def matrixField[T](pathToArray: String, arrayField: ArrayField[T], defaultNumberOfRows: Int): MatrixField[T] = new MatrixFieldFromPathAndArrayField[T](pathToArray, arrayField, defaultNumberOfRows)
  }


//  def findPattern(path: String, in: Map[String, Any]) = {
//    val s = in.getValueFromPath(path).get
//
//    s match {
//      case ba: Map[String, Any] => println("Structure")
//      case ba: Seq[Any] => println("Array")
//      case _ => println("Basic type")
//    }
//
//  }


  //def max(values: Seq[Double]): Double = values.max
  def max(values: Seq[Double]): Double = values.max

  def translate[T](translations: Map[Any, Any])(thisVal: Any): Any = translations.getOrElse(thisVal, thisVal)

  // TODO: translate date to int (timestamp), for example
*/
def scale(scale: Number)(thisVal: Number): Number = thisVal.getDouble * scale.getDouble


  def main(args: Array[String]) = {

    println("aa")

    val f = new BasicField("tarifa", translations = Option(Map("3.0A" -> 1)), transformations = Option(Seq(scale(100))))
    println(f.getValue(inTipo))

//    //findPattern("consumo", inTipo)
//
//    import ArrayField._
//    import Field._
//    import MatrixField._
//
//    val tariff = field[Int]("tarifa", -1, translate(Map("3.0A" -> 4))(_) )
//    println(tariff.getValue(inTipo))
//
//    //val potMaxActa = field[Int]("potMaxActa", 10)
//    //println(potMaxActa.getValue(inTipo))
//
//    val ejemplos = arrayField[Double]("array", arrayField(Seq(
//      field("0", -1.0),
//      field("1", -1.0),
//      field("2", -1.0),
//      field("3", -1.0),
//      field("4", -1.0)
//    )))
//
//    println(ejemplos.getValue(inTipo))
//
//    val ejemplos2 = arrayField[Double]("array", Seq(0.0, 0.0, -1.0, -1.0, -1.0), -9.0)
//    println(ejemplos2.getValue(inTipo))
//
//    val comsumption = matrixField[Double]("consumo", arrayField[Double](Seq(
//
//      field[Double](arrayField(Seq(
//        field("potencias.p1", -1.0, Seq(scale(100.0)_)),
//        field("potencias.p4", -1.0, Seq(scale(100.0)_))
//      )), max _),
//
//      field[Double](arrayField(Seq(
//        field("potencias.p2", -1.0, Seq(scale(100.0)_)),
//        field("potencias.p5", -1.0, Seq(scale(100.0)_))
//      )), max _),
//
//      field[Double](arrayField(Seq(
//        field("potencias.p3", -1.0, Seq(scale(100.0)_)),
//        field("potencias.p6", -1.0, Seq(scale(100.0)_))
//      )), max _)
//    )), 12)
//
//    println(comsumption.getValue(inTipo))
//
//
//    val arrOfArrs = matrixField[Double]("ejemplo", arrayField[Double](Seq(
//      field("0", -1.0),
//      field("1", -1.0)
//    )), 12)
//
//    println(arrOfArrs.getValue(inTipo))



  }
}
