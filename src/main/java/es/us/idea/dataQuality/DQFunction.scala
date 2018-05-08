package es.us.idea.dataQuality

import es.us.idea.dataQuality.internal.businessRules.operations.{AndBRO, NotBRO}
import es.us.idea.dataQuality.internal.businessRules.{FieldMatchesBR, FieldNotNullBR}

import scala.io.Source

object DQFunction {

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

  }
}
