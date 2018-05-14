package es.us.idea.dataQuality

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import es.us.idea.dataQuality.internal.DataQuality
//import es.us.idea.dataQuality.internal.businessRules.basic.numeric.{FieldGreaterEqThanBR, FieldGreaterThanBR, FieldLessEqThanBR, FieldLessThanBR}
//import es.us.idea.dataQuality.internal.businessRules.basic.{FieldBetweenBR, FieldMatchesBR, FieldNotNullBR}
//import es.us.idea.dataQuality.internal.businessRules.operations.{AndBRO, IfBRO, NotBRO}
//import es.us.idea.dataQuality.internal.businessRules.BusinessRule
import es.us.idea.dataQuality.internal.dimension._

import scala.io.Source
import scala.util.Try

object DQFunction {

  val dato: Any = "Empresa distribuidora"

  val inTipo = Map("ICPInstalado" -> "Icp no instalado", "derechosExtension" -> 32.91, "tension" -> "3X220/380V", "propiedadEqMedidaTitular" -> "Empresa distribuidora",
    "potenciaContratada" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 32.91, "p2" -> 32.91, "p1" -> 32.91, "p6" -> 0.0), "impagos" -> "NO", "tipoFrontera" -> 4,
    "tarifa" -> "3.0A", "ubicacionPoblacion" -> "SOMO", "potMaxBie" -> 32.91, "distribuidora" -> "0027 - VIESGO DISTRIBUCION ELECTRICA, S.L.", "fechaAltaSuministro" -> "24/04/1991", "DH" -> "DH3", "totalFacturaActual" -> 4098.68,

    //"propiedadICPTitular" -> "Empresa distribuidora", "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria", "consumo" -> Seq(Map("potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")), "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> null, "potMaxActa" -> 32.91)
    "propiedadICPTitular" -> dato, "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria", "consumo" -> Seq(Map("potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")), "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> null, "potMaxActa" -> 32.91)


  // Soporta nombres de keys con punto. Ej: potenciaContratada.p1 buscaria p1 en el Map anidado potenciaContratada
  def getValueOfKey(key: String, map: Map[String, Any]): Any = {

    if(key.contains(".")) {
      val keySplt = key.split('.')
      if(map.contains(keySplt.head)) {
        return getValueOfKey(keySplt.tail.mkString("."), map.get(keySplt.head).get.asInstanceOf[Map[String, Any]])
      } else {
        return null
      }
    } else {
      return map.get(key).getOrElse(null)
    }
  }

  // Si alguno de los campos especificados en dqin es igual que alguno de los valores especificados en values
  def numberOfMatches(in: Map[String, Any], dqin: Seq[String], values: Seq[Any]): Int = dqin.count(values contains getValueOfKey(_, in))

  // Si alguno de los campos especificados en dqin es nulo
  def numberOfNulls(in: Map[String, Any], dqin: Seq[String]): Int = numberOfMatches(in, dqin, Seq(null))

  // Comprueba si el campo dqin hace match con alguno de los valores
  def fieldMatches(in: Map[String, Any], dqin: String, values: Seq[String]) : Int = numberOfMatches(in, Seq(dqin), values)

  // Comprueba si el campo dqin es nulo
  def fieldNotNull(in: Map[String, Any], dqin: String) : Int = not(numberOfNulls(in, Seq(dqin)))

  // Niega la expresion
  def not(binaryDq: Int): Int = 1 - binaryDq
/*
  def main(args: Array[String]) = {

    val in = Map(
              "a" -> 1,
              "b" -> null,
              "c" -> "a",
              "potenciaContratada" -> Map("p1" -> 1, "p2"-> 18,  "p3" -> "31"),
              "datosContrato" -> Map("region" -> Map("pais" -> "España", "ubicacionProvincia" -> "Asturias", "datosCorreo" -> Map("codigoPostal" -> 12345)), "p2"-> 18,  "p3" -> "31"),
              "tarifa" -> null
            )


    // PRUEBAS

    val n = numberOfMatches(in, Seq("a", "c", "potenciaContratada.p1", "datosContrato.region.datosCorreo.codigoPostal"), Seq(12345, "a"))

    println(s"Matches prueba 1: $n")

    val provinciasEsp = Source.fromFile("/home/alvaro/datasets/provincias_es").getLines.toSeq
    val tarifas = Source.fromFile("/home/alvaro/datasets/tarifas").getLines.toSeq


    //println(provinciasEsp.mkString(", "))
    //println(s"Matches prueba 2: $accuracyProvincia")


    // Calcula la accuracy de la tupla
    def accuracyDataQuality(in: Map[String, Any]): Double = {
      val accuracyProvincia = fieldMatches(in, "datosContrato.region.ubicacionProvincia", provinciasEsp)
      val accuracyTarifa = fieldMatches(in, "tarifa", tarifas)
      return 0.25 * accuracyProvincia + 0.75 * accuracyTarifa
    }

    // Calcula la completness de la tupla
    def completnessDataQuality(in: Map[String, Any]): Double = {
      val completnessProvincia = fieldNotNull(in, "datosContrato.region.ubicacionProvincia")
      val completnessTarifa = fieldNotNull(in, "tarifa")
      return 0.25 * completnessProvincia + 0.75 * completnessTarifa
    }

    def credibilityDataQuality(in: Map[String, Any]): Double = {
      val credibilityProvincia = not(fieldMatches(in, "datosContrato.region.ubicacionProvincia", Seq("Asturias")))
      return 1.0 * credibilityProvincia
    }

    println(s"Accuracy: ${accuracyDataQuality(in)}")
    println(s"Completness: ${completnessDataQuality(in)}")
    println(s"Credibility: ${credibilityDataQuality(in)}")

    val fieldMatchesObj = new FieldMatchesBR("datosContrato.region.ubicacionProvincia", Seq("Asturias"))
    val fieldNotNullObj = new FieldNotNullBR("tarifa")

    val notFieldMatchesObj = new NotBRO(fieldMatchesObj)
    val notNotFieldMatchesObj = new NotBRO(notFieldMatchesObj)

    val andBusinessRules = new AndBRO(Seq(fieldMatchesObj, fieldNotNullObj))

    println(s"Field matches ${fieldMatchesObj.getMetric(in)}")
    println(s"Field negated ${notFieldMatchesObj.getMetric(in)}")
    println(s"Field negated again ${notNotFieldMatchesObj.getMetric(in)}")
    println(s"Field not null ${fieldNotNullObj.getMetric(in)}")
    println(s"And fields ${andBusinessRules.getMetric(in)}")




    val propiedadICPTitular = new FieldMatchesBR("propiedadICPTitular", Seq(null/*, null*/))

    //val propiedadICPTitularCred = new AbstractDQDimension("credibility", Seq(propiedadICPTitular))

    println(s"propiedadICPTitular ${propiedadICPTitular.getMetric(inTipo)}")
    //println(s"propiedadICPTitularCred ${propiedadICPTitularCred.calculateDimensionDQ(inTipo)}")



    val accbr = new AccuracyDQDimension(Seq(new NotBRO(new FieldMatchesBR("ubicacionProvincia", Seq("Asturias")))))

    println(s"Prueba dimension ${accbr.calculateDimensionDQ(inTipo)}")

    val dqqq = new DataQuality(accuracy = Option((0.75, accbr)))

    val v = dqqq.calculateDQ(inTipo, "credibility")
    println(v)


    // *** Pruebas JSON ***

    val jsonStr = """{"type": "matches", "field":"tarifa", "values": ["3.1A", "3.0A"]}"""

    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val jsonBR = objectMapper.readValue[BusinessRule](jsonStr)

    println(s"Prueba desde json ${jsonBR.getMetric(inTipo)}")


    val str = objectMapper.writeValueAsString(accbr)

    println(str)

    //val dsds = objectMapper.readValue[AbstractDQDimension](jsonStr)
    val dsds = objectMapper.readValue[DataQuality](
      """
        |{"accuracy": [0.5, {"type":"accuracy","weightedBusinessRules":[[0.5,{"type":"not", "businessRule": {"type": "matches", "field":"tarifa", "values": ["3.1AA", "3.0AA"]}}]]}]}
      """.stripMargin
    )

    println(s"prueba mas compleja ${dsds.getDqout(inTipo)}")




    val sasa =
      """
        |{
        |  "accuracy": [
        |    0.5,
        |    {
        |      "type": "accuracy",
        |      "weightedBusinessRules":[
        |        [
        |          0.75,
        |          {
        |            "type": "matches",
        |            "field": "tarifa",
        |            "values":
        |            [
        |              "2.0DHA",
        |              "2.1DHA",
        |              "3.1A",
        |              "6.2",
        |              "2.1DHS",
        |              "6.1B",
        |              "6.1A",
        |              "2.1A",
        |              "2.0DHS",
        |              "3.0A",
        |              "6.3",
        |              "2.0A",
        |              "6.4"
        |            ]
        |          }
        |        ],
        |        [
        |          0.25,
        |          {
        |            "type": "matches",
        |            "field": "ubicacionProvincia",
        |            "values":
        |            [
        |              "Cantabria",
        |              "Asturias",
        |              "Ourense"
        |            ]
        |          }
        |        ]
        |      ]
        |    }
        |  ],
        |  "completeness": [
        |    0.25,
        |    {
        |      "type": "completeness",
        |      "weightedBusinessRules":[
        |        [
        |          0.25,
        |          {
        |            "type": "notNull",
        |            "field": "ubicacionProvincia"
        |          }
        |        ],
        |        [
        |          0.75,
        |          {
        |            "type": "notNull",
        |            "field": "tarifa"
        |          }
        |        ]
        |      ]
        |    }
        |  ],
        |  "credibility": [
        |    0.25,
        |    {
        |      "type": "credibility",
        |      "weightedBusinessRules":[
        |        [
        |          1.0,
        |          {
        |            "type": "not",
        |            "businessRule":{
        |              "type": "matches",
        |              "field": "ubicacionProvincia",
        |              "values":
        |              [
        |               "Asturias"
        |              ]
        |            }
        |          }
        |        ]
        |      ]
        |    }
        |  ]
        |}
      """.stripMargin

    val pruebaDefinitivaJson = objectMapper.readValue[DataQuality](sasa)

    println(s"prueba definitiva json ${pruebaDefinitivaJson.getDqout(inTipo)}")



    def toDouble: (Any) => Option[Double] = {case s: String => Try(s.toDouble).toOption case i: Int => Option(i) case f: Float => Option(f) case d: Double => Option(d) }
    //val comp = (Numeric.BigDecimalIsFractional(a) > Numeric.BigDecimalIsFractional(b))

    //println(a.asInstanceOf[Numeric[_]])

    val numericBR = new FieldLessThanBR("potenciaContratada.p1", Seq(32.92))
    val conditionalBR = new IfBRO(new FieldMatchesBR("tarifa", Seq("3.0A")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(50.0)), true)
    val betweenBR = new FieldBetweenBR("potenciaContratada.p1", Seq(32.91, 40))

    val tariffRules = new AndBRO(Seq(
      new IfBRO(new FieldMatchesBR("tarifa", Seq("3.0A")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("3.0A")), new FieldGreaterThanBR("potenciaContratada.p2", Seq(15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("3.0A")), new FieldGreaterThanBR("potenciaContratada.p3", Seq(15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("3.1A")), new FieldLessEqThanBR("potenciaContratada.p1", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("3.1A")), new FieldLessEqThanBR("potenciaContratada.p2", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("3.1A")), new FieldLessEqThanBR("potenciaContratada.p3", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.0DHA")), new FieldLessThanBR("potenciaContratada.p1", Seq(10)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.0DHA")), new FieldLessThanBR("potenciaContratada.p2", Seq(10)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.0DHA")), new FieldLessThanBR("potenciaContratada.p3", Seq(10)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.1DHA")), new FieldBetweenBR("potenciaContratada.p1", Seq(10, 15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.1DHA")), new FieldBetweenBR("potenciaContratada.p2", Seq(10, 15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.1DHA")), new FieldBetweenBR("potenciaContratada.p3", Seq(10, 15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.0DHS")), new FieldLessThanBR("potenciaContratada.p1", Seq(10)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.0DHS")), new FieldLessThanBR("potenciaContratada.p2", Seq(10)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.0DHS")), new FieldLessThanBR("potenciaContratada.p3", Seq(10)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.1DHS")), new FieldBetweenBR("potenciaContratada.p1", Seq(10, 15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.1DHS")), new FieldBetweenBR("potenciaContratada.p2", Seq(10, 15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("2.1DHS")), new FieldBetweenBR("potenciaContratada.p3", Seq(10, 15)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.1A")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.1A")), new FieldGreaterThanBR("potenciaContratada.p2", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.1A")), new FieldGreaterThanBR("potenciaContratada.p3", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.1B")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.1B")), new FieldGreaterThanBR("potenciaContratada.p2", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.1B")), new FieldGreaterThanBR("potenciaContratada.p3", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.2")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.2")), new FieldGreaterThanBR("potenciaContratada.p2", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.2")), new FieldGreaterThanBR("potenciaContratada.p3", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.3")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.3")), new FieldGreaterThanBR("potenciaContratada.p2", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.3")), new FieldGreaterThanBR("potenciaContratada.p3", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.4")), new FieldGreaterThanBR("potenciaContratada.p1", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.4")), new FieldGreaterThanBR("potenciaContratada.p2", Seq(450)), true),
      new IfBRO(new FieldMatchesBR("tarifa", Seq("6.4")), new FieldGreaterThanBR("potenciaContratada.p3", Seq(450)), true)
    ))

    println(s"numeric BR ${numericBR.getMetric(inTipo)}")
    println(s"conditional BR ${conditionalBR.getMetric(inTipo)}")
    println(s"between BR ${betweenBR.getMetric(inTipo)}")
    println(s"Tariff rules BR ${tariffRules.getMetric(inTipo)}")


    val accuracy = new AccuracyDQDimension(
      Seq(
        (0.25, new FieldMatchesBR("ubicacionProvincia", provinciasEsp)),
        (0.75, new FieldMatchesBR("tarifa", tarifas))
      )
    )

    val consistency = new ConsistencyDQDimension(
      Seq(
        (1.0, tariffRules)
      )
    )

    val completness = new CompletenessDQDimension(
      Seq(
        (0.25, new FieldNotNullBR("ubicacionProvincia")),
        (0.75, new FieldNotNullBR("tarifa"))
      )
    )

    val credibility = new CredibilityDQDimension(
      Seq(
        new NotBRO(new FieldMatchesBR("ubicacionProvincia", Seq("Asturias")))
      )
    )

    val dq = new DataQuality(
      accuracy = Option((0.2, accuracy)),
      consistency= Option((0.5, consistency)),
      completeness = Option((0.2, completness)),
      credibility = Option((0.1, credibility))
    )

    println(s"DataQuality: ${dq.getDqout(inTipo)}")
    println()


  }
*/
}
