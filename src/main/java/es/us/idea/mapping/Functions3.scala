package es.us.idea.mapping

import scala.util.Try

object Functions3 {

  val inTipo = Map(
    "array" -> Seq(0.0, 1.0, null, null, null),
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

  // Implicit to avoid Option(null)
  implicit class NotNullOption[T](val t: Try[T]) extends AnyVal {
    def toNotNullOption = t.toOption.flatMap{Option(_)}
  }

  // Implicit declarations
  object Transform {
    def transform[T](f:(Any) => Option[T]) = f
    implicit val transformInt = transform((a) => Try(asInt(a).get).toOption)
    implicit val transformDouble = transform((a) => Try(asDouble(a).get).toOption)
    implicit val transformSeq = transform((a) => Try(asSeq(a).get).toOption)
    implicit val transformMap = transform((a) => Try(asMap(a).get).toOption)
  }

  implicit class AnyType(a: Any) {
    def getAs[T]()(implicit run: Any => Option[T]): Option[T] = run(a)
  }



  def basicType[T](from: String, in: Map[String, Any], default: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T]) : T = {
    val s1 = getValueOfKey(from, in)
    if(s1.isDefined) {
      val s2 = if(transformations.isDefined) transformations.get.getOrElse(s1.get, s1.get) else s1.get
      s2.getAs[T].getOrElse(default)
    } else default
  }

  // Helper for basicArray and basicMatrix
  def buildBasicArray[T](value: Any, default: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]]) : Seq[T] = {
    val s2 = value.getAs[Seq[Any]]
    if(s2.isDefined) {
      s2.get.zipWithIndex.map{ case (e, i) => e.getAs[T].getOrElse(default.lift(i).getOrElse(defaultVal)) }
    }
    else default
  }

  def basicArray[T](from: String, in: Map[String, Any], default: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]]) : Seq[T] = {
    val s1 = getValueOfKey(from, in)
    if(s1.isDefined) buildBasicArray(s1.get, default, defaultVal, transformations)
    else default
  }

  def buildArrayFromFields[T](fromFields: Seq[String], in: Map[String, Any], default: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]]) : Seq[T] = {
    fromFields.zipWithIndex.map{case (e, i) => basicType(e, in, default.lift(i).getOrElse(defaultVal), transformations)}
  }

  def basicMatrix[T](from: String, in: Map[String, Any], default: Seq[Seq[T]], defaultArr: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]]) : Seq[Seq[T]] = {
    val s1 = getValueOfKey(from, in)
    if(s1.isDefined){
      val s2 = s1.get.getAs[Seq[Any]]
      if(s2.isDefined) s2.get.map(x => buildBasicArray[T](x, defaultArr, defaultVal, transformations))
      else default
    } else default
  }

  def buildMatrixFromArrayFieldInArray[T](from: String, nestedArrayField: String, in: Map[String, Any], default: Seq[Seq[T]], defaultArr: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]], ev3: Any => Option[Map[String, Any]]) : Seq[Seq[T]] = {
    val s1 = getValueOfKey(from, in)
    if(s1.isDefined) {
      val s2 = s1.get.getAs[Seq[Any]]
      if(s2.isDefined){
        s2.get.map(x => {
          val s3 = x.getAs[Map[String, Any]]
          if(s3.isDefined) basicArray[T](nestedArrayField, s3.get, defaultArr, defaultVal, transformations) else defaultArr
        })
      } else default
    } else default
  }

  // caso de conquense, hidrocantabrico
  def buildMatrixFromFieldsInArray[T](from: String, nestedFields: Seq[String], in: Map[String, Any], default: Seq[Seq[T]], defaultArr: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]], ev3: Any => Option[Map[String, Any]]) : Seq[Seq[T]] = {
    val s1 = getValueOfKey(from, in)
    if(s1.isDefined) {
      val s2 = s1.get.getAs[Seq[Any]]
      if(s2.isDefined) {
        s2.get.map(x => {
          val s3 = x.getAs[Map[String, Any]]
          if(s3.isDefined) nestedFields.map(x => basicType(x, s3.get, defaultVal, transformations)) else defaultArr
        })
      } else default
    } else default
  }

  // caso de raw endesa
  def buildMatrixFromFields[T](fromFields: Seq[Seq[String]], in: Map[String, Any], defaultArr: Seq[T], defaultVal: T, transformations: Option[Map[Any, T]] = None )(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]]) : Seq[Seq[T]] = {
    fromFields.map(x => buildArrayFromFields[T](x, in, defaultArr, defaultVal, transformations))
  }


  // def buildMatrixFromFields

  def main(args: Array[String]) = {

    import Transform._
    //println(basicType[Int]("tarifa", inTipo, 0, Option(Map("3.0A" -> 2))))
    //println(basicArray[Double]("array", inTipo, Seq(0.0, 2.0, 3.0), -9999.0))
    //println(buildArrayFromFields[Double](Seq("potMaxActa", "potenciaContratada.p1", "potenciaContratada.p2", "potenciaContratada.p3", "jajaja"), inTipo, Seq(0.0, 0.0, 0.0, 0.0), -9999.0))
    //println(basicMatrix[Double]("ejemplo", inTipo, Seq(Seq(0.0, 1.0)), Seq(0.0, 0.0, 0.0, 0.0), -9999.0))
    //println(buildMatrixFromArrayFieldInArray[Double]("consumo", "arrayAnidado", inTipo, Seq(Seq(0.0, 1.0)), Seq(0.0, 0.0, 0.0, 0.0), -9999.0))
    println(buildMatrixFromFieldsInArray[Double]("consumo", Seq("potencias.p1", "potencias.p2", "potencias.p3", "potencias.p4", "potencias.p5", "potencias.p6"), inTipo, Seq(Seq(0.0, 1.0)), Seq(0.0, 0.0, 0.0, 0.0), -9999.0))
    //println(buildMatrixFromFields[Int](Seq(Seq("ICPInstalado", "tarifa"), Seq("derechosExtension", "potMaxActa")), inTipo, Seq(50, 60), 1000000, Option(Map("Icp no instalado" -> 0, "3.0A" -> 3))))
  }



  def getValueOfKey(key: String, map: Map[String, Any]): Option[Any] = {
    if (key.contains(".")) {
      val keySplt = key.split('.')
      if (map.contains(keySplt.head)) {
        return getValueOfKey(keySplt.tail.mkString("."), map.get(keySplt.head).get.asInstanceOf[Map[String, Any]])
      } else {
        return null
      }
    } else {
      return map.get(key) // .getOrElse(null)
    }
  }


  // Convertions
  def asDouble(value: Any): Option[Double] = {
    value match {
      case s: String => Try(s.toDouble).toNotNullOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }
  def asInt(value: Any): Option[Int] = Try(asDouble(value).get.toInt).toNotNullOption
  def asSeq(value: Any): Option[Seq[Any]] = Try(value.asInstanceOf[Seq[Any]]).toNotNullOption
  def asMap(value: Any): Option[Map[String, Any]] = Try(value.asInstanceOf[Map[String, Any]]).toNotNullOption

}
