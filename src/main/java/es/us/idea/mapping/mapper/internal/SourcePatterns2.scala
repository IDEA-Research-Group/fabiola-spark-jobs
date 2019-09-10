package es.us.idea.mapping.mapper.internal

import Helpers._

import scala.util.Try

object SourcePatterns2 {

  val inTipo = Map(
    "id" -> 500,
    "array" -> Seq(0.0, 1.0, null, 80, null),
    "ejemplo" -> Seq(Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(null, 1.0), null, Seq(9.8, 1.0) ),
    //"ejemplo" -> Seq(),
    //"ejemplo" -> Seq(Seq()),
    "ICPInstalado" -> "Icp no instalado", "derechosExtension" -> 32.91, "tension" -> "3X220/380V", "propiedadEqMedidaTitular" -> "Empresa distribuidora",
    "potenciaContratada" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 34.91, "p2" -> 32.91, "p1" -> 32.91, "p6" -> 0.0), "impagos" -> "NO", "tipoFrontera" -> 4,
    "tarifa" -> "3.0A", "ubicacionPoblacion" -> "SOMO", "potMaxBie" -> 32.91, "distribuidora" -> "0027 - VIESGO DISTRIBUCION ELECTRICA, S.L.", "fechaAltaSuministro" -> "24/04/1991", "DH" -> "DH3", "totalFacturaActual" -> 4098.68,
    "propiedadICPTitular" -> "Empresa distribuidora", "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria",
    "consumo" -> Seq(
      Map("arrayAnidado" -> Seq(1.0, 5.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014"),
      Map("arrayAnidado" -> Seq(2.0, 10.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 10.0, "p2" -> 20.0, "p1" -> 20.0, "p6" -> 0.0), "anio" -> 2015, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")
    ),
    "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> 32.91)

  //trait FieldTrait

  trait Field {
    def getNumber(in: Map[String, Any])/*(implicit ev: Option[Any] => Option[Number])*/: Option[Number]
    // TODO getString & getNumber???
    def getDefault(): Option[Number]
  }

  trait ArrayField {
    def getNumber (in: Map[String, Any])/*(implicit ev: Option[Any] => Option[Number])*/: Option[Seq[Option[Number]]]
    def getDefault(): Option[Seq[Option[Number]]]
  }

  trait MatrixField {
    def getNumber(in: Map[String, Any])/*(implicit ev: Option[Any] => Option[Number])*/: Option[Seq[Option[Seq[Option[Number]]]]]
    def getDefault(): Option[Seq[Option[Seq[Option[Number]]]]]
  }

  // FIELD
  class BasicField(path: String, default: Option[Number]=None, translations: Option[Any => Number] = None, transformations: Option[Seq[Number => Number]] = None) extends Field {
    override def getNumber(in: Map[String, Any])/*(implicit ev: Option[Any] => Option[Number])*/: Option[Number] =
      in.getValueFromPath(path, default).getAs[Number](translations).applyPipeline(transformations.getOrElse(Seq()))
    override def getDefault(): Option[Number] = default
  }

  class BasicFromArrayField(arrayField: ArrayField, reduction: Seq[Number] => Number) extends Field {
    override def getNumber(in: Map[String, Any])/*(implicit ev: Any => Option[Number])*/: Option[Number] =
      arrayField.getNumber(in).applyReduction(reduction) // Apply pipeline
      // arrayField.getValue(in).applyReduction(reduction) // Apply pipeline
    override def getDefault(): Option[Number] = arrayField.getDefault().applyReduction(reduction)
  }

  //object Field {
  //  def field[T](path: String, default: T): Field[T] = new BasicField[T](path, default)
  //  def field[T](path: String, default: T, translations: Any => Any): Field[T] = new BasicField[T](path, default, Option(translations))
  //  def field[T](path: String, default: T, transformations: Seq[T => T]): Field[T] = new BasicField[T](path, default, None, Option(transformations))
  //  def field[T](path: String, default: T, translations: Any => Any, transformations: Seq[T => T]): Field[T] = new BasicField[T](path, default, Option(translations), Option(transformations))
  //
  //  def field[T](arrayField: ArrayField[T], reduction: Seq[T] => T): Field[T] = new BasicFromArrayField[T](arrayField, reduction)
  //}





  // ARRAY FIELD
  // GET DEFAULT FROM INDEX; DEFAULT ARRAY AND DEFAULT VALUE
  def getDefaultElementOfArray(index: Int, defaultArr: Option[Seq[Number]], default: Option[Number]) = {
    if(defaultArr.isDefined)
      defaultArr.get.lift(index).orElse(default)
    else default
  }

  class ArrayFieldFromFields(fields: Seq[Field]) extends ArrayField {
    override def getNumber(in: Map[String, Any])/*(implicit ev: Any => Option[T])*/: Option[Seq[Option[Number]]] =
      Option(fields.map(_.getNumber(in)))
    override def getDefault(): Option[Seq[Option[Number]]] = Option(fields.map(_.getDefault()))
  }


  class ArrayFieldFromPath(path: String, defaultArr: Option[Seq[Number]], default: Option[Number]) extends ArrayField {
    override def getNumber(in: Map[String, Any]): Option[Seq[Option[Number]]] = {
      val seqOpt = in.getValueFromPath(path, defaultArr).getAs[Seq[Any]]()
      if(seqOpt.isDefined) Option(seqOpt.get.zipWithIndex.map{ case (e, i) => Number.createNumber(e).orElse(getDefaultElementOfArray(i, defaultArr, default))})
      else getDefault()
    }

    override def getDefault(): Option[Seq[Option[Number]]]= if(defaultArr.isDefined) Option(defaultArr.get.wrap()) else None
  }

  // path is expected to be an array of basic types and fields the indexes of that array
  class ArrayFieldFromPathAndFields(path: String, fields: ArrayField) extends ArrayField {
    override def getNumber(in: Map[String, Any])/*(implicit ev: Any => Option[T])*/: Option[Seq[Option[Number]]] = {
      println(path)
      val defaultArr = fields.getDefault()
      val seqOpt = in.getValueFromPath(path, defaultArr).getAs[Seq[Any]]()
      if(seqOpt.isDefined)
        fields.getNumber(
          seqOpt.get
            .zipWithIndex
            .map(el => (el._2.toString, el._1))
            .toMap
        )
      else getDefault()
    }
    override def getDefault(): Option[Seq[Option[Number]]] = fields.getDefault()
  }

  // TODO implementar cuando implemente un campo matriz
  class ArrayFieldFromMatrix(matrixField: MatrixField, reduction: Seq[Seq[Number]] => Seq[Number]) extends ArrayField {
    override def getNumber(in: Map[String, Any])/*(implicit ev: Any => Option[T])*/: Option[Seq[Option[Number]]] =
      matrixField.getNumber(in).applyReduction(reduction)
    override def getDefault(): Option[Seq[Option[Number]]] = matrixField.getDefault().applyReduction(reduction)
  }

//  object ArrayField {
//    def arrayField[T](fields: Seq[Field[T]]): ArrayField[T] = new ArrayFieldFromFields(fields)
//
//    def arrayField[T](path: String, defaultArr: Seq[T], default: T): ArrayField[T] = new ArrayFieldFromPath[T](path, defaultArr, default)
//
//    def arrayField[T](path: String, fields: ArrayField[T]): ArrayField[T] = new ArrayFieldFromPathAndFields[T](path, fields)
//
//    def arrayField[T](matrixField: MatrixField[T], reduction: Seq[Seq[T]] => Seq[T]) = new ArrayFieldFromMatrix[T](matrixField, reduction)
//  }


  // MATRIX FIELD
/*
  class MatrixFieldFromArrayFields[T](arrayFields: Seq[ArrayField]) extends MatrixField {
    override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[Seq[T]] =
      arrayFields.map(_.getValue(in))
    override def getDefault(): Seq[Seq[T]] = arrayFields.map(_.getDefault())
  }*/

  class MatrixFieldFromPathAndArrayField(pathToArray: String, arrayField: ArrayField, defaultNumberOfRows: Option[Int]) extends MatrixField {
    override def getNumber(in: Map[String, Any])/*(implicit ev: Any => Option[T])*/: Option[Seq[Option[Seq[Option[Number]]]]] = {
      val optSeq = in.getValueFromPath(pathToArray, getDefault()).getAs[Seq[Any]]()

        if(optSeq.isDefined) {
          Try(optSeq.get.map(x => {
            if (x.isInstanceOf[Seq[Any]]) {
              val nestedSeq = Option(x).getAs[Seq[Any]]()
              if (nestedSeq.isDefined) new ArrayFieldFromPathAndFields("array", arrayField).getNumber(Map("array" -> nestedSeq.get))
              else arrayField.getDefault()
            } else {
              val map = Option(x).getAs[Map[String, Any]]()
              if (map.isDefined) arrayField.getNumber(map.get)
              else arrayField.getDefault()
            }
          })).toOption // Realmente el Try no haria falta, bastaria con castear a Option pero lo voy a dejar por si hay alguna excepcion no controlada.
        } else getDefault()

      // Para cada caso, ha de diferenciar si cada elemento de la secuencia (Any) es otra secuencia o un map
      //if(seq.isDefined) {
      //  seq.get.map(x => {
      //    if(x.isInstanceOf[Seq[Any]]) {
      //      // Tratar como un array
      //      arrayField.getValue(
      //        x.getAsOrElse[Seq[Any]](arrayField.getDefault())
      //          .zipWithIndex
      //          .map(el => (el._2.toString, el._1))
      //          .toMap
      //      )
      //    } else {
      //      // Tratar como una estructura
      //      val map = x.getAs[Map[String, Any]]()
      //      if(map.isDefined) {
      //        arrayField.getValue(map.get)
      //      } else arrayField.getDefault()
      //    }
      //  })
      //} else getDefault()


    }
    override def getDefault(): Option[Seq[Option[Seq[Option[Number]]]]] =  if(defaultNumberOfRows.isDefined) Option(Seq.fill[Option[Seq[Option[Number]]]](defaultNumberOfRows.get)(arrayField.getDefault())) else None
  }
/*
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

  def max(values: Seq[Number])(implicit ordering: Ordering[Number]) = values.max

  def main(args: Array[String]) = {

    println("aa")

    val f = new BasicField("tarifa", translations = Option(Map("3.0A" -> 1)), transformations = Option(Seq(scale(100))))
    println(f.getNumber(inTipo))

    val fromArr = new BasicFromArrayField(new ArrayFieldFromFields(Seq(
      new BasicField("potenciaContratada.p1"),
      new BasicField("potenciaContratada.p2"),
      new BasicField("potenciaContratada.p3"),
      new BasicField("potenciaContratada.p333", Option(9999))
    )), max)

    println(fromArr.getNumber(inTipo))

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
