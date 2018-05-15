package es.us.idea.dataQuality

import java.lang.reflect.InvocationTargetException

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import com.mongodb.spark.sql.helpers.StructFields
import es.us.idea.cop._
import es.us.idea.dataQuality.DQTests.{provinciasEsp, tarifas}
import es.us.idea.dataQuality.internal.DataQuality
import es.us.idea.dataQuality.internal.businessRules.BusinessRule
import es.us.idea.dataQuality.internal.conditions.{If, True}
import es.us.idea.dataQuality.internal.conditions.operators.{And, Not}
import es.us.idea.dataQuality.internal.conditions.valConditions.{Between, Matches, NotNull}
import es.us.idea.dataQuality.internal.conditions.valConditions.numeric.{GreaterThan, LessEqThan, LessThan}
import es.us.idea.dataQuality.internal.decisionRules.{DecisionRule, DecisionRulesEngine}
import es.us.idea.dataQuality.internal.dimension._
import es.us.idea.utils.{Datasources, FabiolaDatabase, SparkRowUtils, Utils}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.functions._

object DQJob {
  //val provinciasEsp = Source.fromFile("/home/alvaro/datasets/provincias_es").getLines.toSeq
  val provinciasEsp = Seq("Araba", "Álava", "Albacete", "Alicante", "Alacant", "Almería", "Ávila", "Badajoz", "Balears (Illes)", "Barcelona", "Burgos", "Cáceres", "Cádiz", "Castellón", "Castelló", "Ciudad Real", "Córdoba", "Coruña (A)", "Cuenca", "Girona", "Granada", "Guadalajara", "Gipuzkoa", "Huelva", "Huesca", "Jaén", "León", "Lleida", "Rioja (La)", "Lugo", "Madrid", "Málaga", "Murcia", "Navarra", "Ourense", "Asturias", "Palencia", "Palmas (Las)", "Pontevedra", "Salamanca", "Santa Cruz de Tenerife", "Cantabria", "Segovia", "Sevilla", "Soria", "Tarragona", "Teruel", "Toledo", "Valencia", "València", "Valladolid", "Bizkaia", "Zamora", "Zaragoza", "Ceuta", "Melilla")

  //val tarifas = Source.fromFile("/home/alvaro/datasets/tarifas").getLines.toSeq
  val tarifas = Seq("2.0DHA", "2.1DHA", "3.1A", "6.2", "2.1DHS", "6.1B", "6.1A", "2.1A", "2.0DHS", "3.0A", "6.3", "2.0A", "6.4")

  def main(args: Array[String]) = {


    // Definicion del objeto de dq
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
        new DecisionRule(new Between("dq", 0.75, 1.0), "bueno"),
        new DecisionRule(new And(Seq(new LessThan("dq", 0.5), new GreaterThan("dq", 0.75))), "regular")
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
    // fin



    var sparkBuilder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"DataQuality-Experimental")
      // .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      // .config("spark.mongodb.output.uri", s"${Utils.removeLastSlashes(fabiolaDBUri)}/$fabiolaDBName.results")

    val spark = sparkBuilder.getOrCreate()

    val dqFunction = (row: Row) => {
      val dqin = SparkRowUtils.fromRowToMap(row)
      dq.getDqout(dqin)
    }

    val executeDQUdf = udf((row: Row) => {
      dqFunction(row)
    })

    val dqin = Seq("ubicacionProvincia", "tarifa", "propiedadICPTitular", "potenciaContratada").map(col(_))
    // TODO OJO: siempre va a devolver las cuatro métricas. Esto serviria para para convertir la lista a columnas
    //val dqout = Seq("dataQuality", "accuracy", "completness", "credibility", "consistency").zipWithIndex.map(x => col("tempdqout").getItem(x._2).as(x._1))

    val ds = spark.read.json("/home/alvaro/datasets/hidrocantabrico_split.json")
    //val hidrocantabrico = spark.read.json("/home/alvaro/datasets/endesa_datos_agregados_split.json")
    //  .map()
    .withColumn("dqout", explode(array(executeDQUdf(struct(dqin: _*))))) // TODO Bastaría con esta column
      .withColumn("dqout.dataQuality", col("dqout.dataQuality"))
      .withColumn("dqout.accuracy", col("dqout.accuracy"))
      .withColumn("dqout.completness", col("dqout.completeness"))
      .withColumn("dqout.credibility", col("dqout.credibility"))
      .withColumn("dqout.consistency", col("dqout.consistency"))
    //    .withColumn("dqout", struct(dqout: _*))

    ds.printSchema
    ds.show(500)
    //hidrocantabrico.select("tarifa").distinct.show

  }
}