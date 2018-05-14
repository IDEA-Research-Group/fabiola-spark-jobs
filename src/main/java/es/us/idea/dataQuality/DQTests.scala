package es.us.idea.dataQuality

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import es.us.idea.dataQuality.internal.DataQuality
import es.us.idea.dataQuality.internal.businessRules.BusinessRule
import es.us.idea.dataQuality.internal.conditions.{If, True}
import es.us.idea.dataQuality.internal.conditions.operators.{And, Not, Or}
import es.us.idea.dataQuality.internal.conditions.valConditions.numeric.{GreaterThan, LessEqThan, LessThan}
import es.us.idea.dataQuality.internal.conditions.valConditions.{Between, Matches, NotNull}
import es.us.idea.dataQuality.internal.decisionRules.{DecisionRule, DecisionRulesEngine}
import es.us.idea.dataQuality.internal.dimension.{AccuracyDQDimension, CompletenessDQDimension, ConsistencyDQDimension, CredibilityDQDimension}

import scala.io.Source
import scala.util.Try

object DQTests {
  val dato: Any = "Empresa distribuidora"

  val provinciasEsp = Source.fromFile("/home/alvaro/datasets/provincias_es").getLines.toSeq
  val tarifas = Source.fromFile("/home/alvaro/datasets/tarifas").getLines.toSeq


  val inTipo = Map("ICPInstalado" -> "Icp no instalado", "derechosExtension" -> 32.91, "tension" -> "3X220/380V", "propiedadEqMedidaTitular" -> "Empresa distribuidora",
    "potenciaContratada" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 32.91, "p2" -> 32.91, "p1" -> 32.91, "p6" -> 0.0), "impagos" -> "NO", "tipoFrontera" -> 4,
    "tarifa" -> "3.0A", "ubicacionPoblacion" -> "SOMO", "potMaxBie" -> 32.91, "distribuidora" -> "0027 - VIESGO DISTRIBUCION ELECTRICA, S.L.", "fechaAltaSuministro" -> "24/04/1991", "DH" -> "DH3", "totalFacturaActual" -> 4098.68,

    //"propiedadICPTitular" -> "Empresa distribuidora", "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria", "consumo" -> Seq(Map("potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")), "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> null, "potMaxActa" -> 32.91)
    "propiedadICPTitular" -> dato, "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria", "consumo" -> Seq(Map("potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")), "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> null, "potMaxActa" -> 32.91)

  def main(args: Array[String]) = {

    val tariffRules = new And(Seq(
      new If(new Matches("tarifa", Seq("3.0A")), new GreaterThan("potenciaContratada.p1", Seq(15)), new True()),
      new If(new Matches("tarifa", Seq("3.0A")), new GreaterThan("potenciaContratada.p2", Seq(15)), new True()),
      new If(new Matches("tarifa", Seq("3.0A")), new GreaterThan("potenciaContratada.p3", Seq(15)), new True()),
      new If(new Matches("tarifa", Seq("3.1A")), new LessEqThan("potenciaContratada.p1", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("3.1A")), new LessEqThan("potenciaContratada.p2", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("3.1A")), new LessEqThan("potenciaContratada.p3", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("2.0DHA")), new LessThan("potenciaContratada.p1", Seq(10)), new True()),
      new If(new Matches("tarifa", Seq("2.0DHA")), new LessThan("potenciaContratada.p2", Seq(10)), new True()),
      new If(new Matches("tarifa", Seq("2.0DHA")), new LessThan("potenciaContratada.p3", Seq(10)), new True()),
      new If(new Matches("tarifa", Seq("2.1DHA")), new Between("potenciaContratada.p1", 10, 15), new True()),
      new If(new Matches("tarifa", Seq("2.1DHA")), new Between("potenciaContratada.p2", 10, 15), new True()),
      new If(new Matches("tarifa", Seq("2.1DHA")), new Between("potenciaContratada.p3", 10, 15), new True()),
      new If(new Matches("tarifa", Seq("2.0DHS")), new LessThan("potenciaContratada.p1", Seq(10)), new True()),
      new If(new Matches("tarifa", Seq("2.0DHS")), new LessThan("potenciaContratada.p2", Seq(10)), new True()),
      new If(new Matches("tarifa", Seq("2.0DHS")), new LessThan("potenciaContratada.p3", Seq(10)), new True()),
      new If(new Matches("tarifa", Seq("2.1DHS")), new Between("potenciaContratada.p1", 10, 15), new True()),
      new If(new Matches("tarifa", Seq("2.1DHS")), new Between("potenciaContratada.p2", 10, 15), new True()),
      new If(new Matches("tarifa", Seq("2.1DHS")), new Between("potenciaContratada.p3", 10, 15), new True()),
      new If(new Matches("tarifa", Seq("6.1A")), new GreaterThan("potenciaContratada.p1", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.1A")), new GreaterThan("potenciaContratada.p2", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.1A")), new GreaterThan("potenciaContratada.p3", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.1B")), new GreaterThan("potenciaContratada.p1", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.1B")), new GreaterThan("potenciaContratada.p2", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.1B")), new GreaterThan("potenciaContratada.p3", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.2")), new GreaterThan("potenciaContratada.p1", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.2")), new GreaterThan("potenciaContratada.p2", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.2")), new GreaterThan("potenciaContratada.p3", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.3")), new GreaterThan("potenciaContratada.p1", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.3")), new GreaterThan("potenciaContratada.p2", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.3")), new GreaterThan("potenciaContratada.p3", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.4")), new GreaterThan("potenciaContratada.p1", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.4")), new GreaterThan("potenciaContratada.p2", Seq(450)), new True()),
      new If(new Matches("tarifa", Seq("6.4")), new GreaterThan("potenciaContratada.p3", Seq(450)), new True())
    ))

    val dte = new DecisionRulesEngine(
      Seq(
        new DecisionRule(new Between("dq", 0.6, 1.0), "bueno"),
        new DecisionRule(new And(Seq(new LessThan("dq", 0.6), new GreaterThan("dq", 0.5))), "regular")
      ),
      "malo"
    )

    val dqDecisionRules = new DecisionRulesEngine(
      Seq(
        new DecisionRule(new Matches("consistency", "malo"), "malo"),
        new DecisionRule(new And(Seq(new Matches("consistency", "regular"), new Matches("credibility", "malo"))), "malo"),
        new DecisionRule(new And(Seq(new Matches("consistency", "regular"), new Matches("credibility", "regular"))), "regular"),
        new DecisionRule(new And(Seq(new Matches("consistency", "regular"), new Matches("credibility", "bueno"))), "regular"),
        new DecisionRule(new And(Seq(new Matches("consistency", "bueno"), new Matches("completitud", "malo"))), "regular"),
        new DecisionRule(new And(Seq(new Matches("consistency", "bueno"), new Matches("completitud", "regular"))), "bueno"),
        new DecisionRule(new And(Seq(new Matches("consistency", "bueno"), new Matches("completitud", "bueno"))), "bueno")
      ),
      "bueno"
    )

    val accuracyDim = new AccuracyDQDimension(
      0.2,
      Seq(
        new BusinessRule(0.5, new Matches("ubicacionProvincia", provinciasEsp)),
        new BusinessRule(0.5, new Matches("tarifa", tarifas))
      ),
      Option(dte)
    )

    val consistencyDim = new ConsistencyDQDimension(
      0.5,
      Seq(
        new BusinessRule(1.0, tariffRules)
      ),
      Option(dte)
    )

    val completnessDim = new CompletenessDQDimension(
      0.2,
      Seq(
        new BusinessRule(0.25, new NotNull("ubicacionProvincia")),
        new BusinessRule(0.75, new NotNull("tarifa"))
      ),
      Option(dte)
    )

    val credibilityDim = new CredibilityDQDimension(
      0.1,
      Seq(
        new BusinessRule(1.0, new Not(new Matches("ubicacionProvincia", Seq("Asturias"))))
      ),
      Option(dte)
    )

    val dq = new DataQuality(
      accuracy = Option(accuracyDim),
      consistency = Option(consistencyDim),
      completeness = Option(completnessDim),
      credibility = Option(credibilityDim),
      decisionRules = Option(dqDecisionRules)
    )

    println(s"DataQuality: ${dq.getDqout(inTipo)}")
    println()

    //val matchTest = new WeightedBusinessRule(0.5, new And(Seq(new Not(new Matches("potenciaContratada.p3", 32.92)), new Matches("potenciaContratada.p6", 0))))
    //val matchTest2 = new WeightedBusinessRule(0.5, new Not(new Matches("propiedadICPTitular", Seq("Empresa distribuidora", null))))


    println(s"dq ${dq.getDqout(inTipo)}")

    //println(s"cualitativo ${accuracyDim.getQualitativeDQ(inTipo)}")


    val jsonStr =
      """
        |{
        |  "accuracy": {
        |    "type": "accuracy",
        |    "weight": 0.2,
        |    "businessRules": [
        |      {
        |        "weight": 0.5,
        |        "condition": {
        |          "type": "matches",
        |          "key": "ubicacionProvincia",
        |          "values": [
        |            "Asturias",
        |            "Burgos",
        |            "Cantabria",
        |            "Lugo",
        |            "Las demás provincias"
        |          ]
        |        }
        |      },
        |      {
        |        "weight": 0.5,
        |        "condition": {
        |          "type": "matches",
        |          "key": "ubicacionProvincia",
        |          "values": [
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
        |          ]
        |        }
        |      }
        |    ],
        |    "decisionRules": {
        |      "decisionRules": [
        |        {
        |          "condition": {
        |            "type": "between",
        |            "key": "dq",
        |            "lowerBound": 0.6,
        |            "higherBound": 1.0
        |          }
        |        },
        |        {
        |          "condition": {
        |            "type": "and",
        |            "conditions": [
        |              {
        |                "type": "lt",
        |                "key": "dq",
        |                "values": [0.6]
        |              },
        |              {
        |                "type": "gt",
        |                "key": "dq",
        |                "values": [0.5]
        |              }
        |            ]
        |          }
        |        }
        |      ],
        |      "default": "malo"
        |    }
        |  }
        |}
      """.stripMargin

    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val jsonBR = objectMapper.readValue[DataQuality](jsonStr)

    println(s"Prueba desde json ${jsonBR.getDqout(inTipo)}")

  }
}
