package es.us.idea.mapping

import reflect.runtime.universe._
import scala.util.Try

object Functions {

  val inTipo = Map(
    "array" -> Seq(0.0, 1.0),
    "ejemplo" -> Seq(Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(0.0, 1.0)),
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
    "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> null, "potMaxActa" -> 32.91)


//  trait Base[T <: Base[T]]
//
//  class DoubleType(v: Double) extends Base[DoubleType]

  object Parse {
    def parse[T](f:Any => Option[T]) = f
    implicit val parseInt = parse(s => Try(asInt(s).get).toOption)
    implicit val parseDouble = parse(s => Try(asDouble(s).get).toOption)
  }

  implicit class AnyType(a: Any) {
    def getAs[T]()(implicit run: Any => Option[T]): Option[T] = run(a)
  }

  def main(args: Array[String]) = {
    //import Parse._
    //"".getAs[Double].foreach(println)
    import Parse._
    println(basicType[Double]("potenciaContratada.p1", inTipo, -99.9, Map()))
  }

  def basicType[T](from: String, in: Map[String, Any], default: T, transformations: Map[Any, T])(implicit run: Any => Option[T]): T = {
    val s1 = getValueOfKey(from, in)
    //val t1 = applyTransformation(s1, transformations)

    s1.get.getAs[T].getOrElse(default)
  }

  def get[T : TypeTag](value: Option[Any], default: T): T = {
    value.get match{
      case x if typeOf[T] <:< typeOf[Int] => asInt(value.get).get.asInstanceOf[T]
      case x if typeOf[T] <:< typeOf[Double] => asDouble(value.get).get.asInstanceOf[T]
      case _ => default
    }
  }



//  def returnIntType(): IntType = new IntType(1)


  def applyTransformation[T](value: Option[Any], transformations: Map[Any, T]): Option[T] = {
    Try(transformations.get(value.get).get).toOption
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

  def asDouble(value: Any): Option[Double] = {
    value match {
      case s: String => Try(s.toDouble).toOption
      case i: Int => Option(i)
      case l: Long => Option(l)
      case f: Float => Option(f)
      case d: Double => Option(d)
      case _ => None
    }
  }

  def asInt(value: Any): Option[Int] = Try(asDouble(value).get.toInt).toOption



}
