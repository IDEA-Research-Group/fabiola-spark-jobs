package es.us.idea.mapping.tests

import es.us.idea.mapping.Utils
import es.us.idea.mapping.internal.array.collaborators.BasicArrayDataMap
import es.us.idea.mapping.internal.basic.{DoubleBasicDataMap, IntBasicDataMap}
import es.us.idea.mapping.internal.matrix.{DoubleMatrixDataMap, IntMatrixDataMap}
import es.us.idea.mapping.internal2.types.array.IntArrayDM
import es.us.idea.mapping.internal2.{ArrayMapping, BasicMapping}
import es.us.idea.mapping.internal2.types.basic.DoubleDM

import scala.util.Try

object QuickTests {

  case class DataMapping(from: String, to: String, toType: String, mapValues: Option[MapStrValues])
  case class MapStrValues()

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

  def main(args: Array[String]) = {

    //val pcP1 = new DoubleBasicDataMap("potenciaContratada.p1", "pcP1").get(inTipo)

    val pcP1 = new BasicMapping[Double]("potenciaContratada.p1", "pcP1", new DoubleDM(-99999.0)).getValue(inTipo).get

    println(pcP1)

    val ejem = new ArrayMapping[Int]("array", "arrayexample", new IntArrayDM(Seq(-999, -999), -888)).getValue(inTipo).get

    println(ejem)

    //val consMatrix = new DoubleMatrixDataMap("ejemplo", None, "consumption").get(inTipo)
    //val consMatrix = new DoubleMatrixDataMap("consumo", Option(new FieldsToArrayDataMap(Seq("potencias.p1", "potencias.p2", "potencias.p3"))), "consumption").get(inTipo)
    //val consMatrix = new IntMatrixDataMap("consumo", Option(new FieldsToArrayDataMap(Seq("anio"))), "consumption").get(inTipo)
    //val consMatrix = new DoubleMatrixDataMap("consumo", Option(new BasicArrayDataMap("arrayAnidado")), "consumption").get(inTipo)

    //println(consMatrix)

  }

}
