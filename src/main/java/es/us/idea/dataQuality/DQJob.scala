package es.us.idea.dataQuality


import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
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
  //val provinciasEsp = Seq("Araba", "Álava", "Albacete", "Alicante", "Alacant", "Almería", "Ávila", "Badajoz", "Balears (Illes)", "Barcelona", "Burgos", "Cáceres", "Cádiz", "Castellón", "Castelló", "Ciudad Real", "Córdoba", "Coruña (A)", "Cuenca", "Girona", "Granada", "Guadalajara", "Gipuzkoa", "Huelva", "Huesca", "Jaén", "León", "Lleida", "Rioja (La)", "Lugo", "Madrid", "Málaga", "Murcia", "Navarra", "Ourense", "Asturias", "Palencia", "Palmas (Las)", "Pontevedra", "Salamanca", "Santa Cruz de Tenerife", "Cantabria", "Segovia", "Sevilla", "Soria", "Tarragona", "Teruel", "Toledo", "Valencia", "València", "Valladolid", "Bizkaia", "Zamora", "Zaragoza", "Ceuta", "Melilla")

  //val tarifas = Source.fromFile("/home/alvaro/datasets/tarifas").getLines.toSeq
  //val tarifas = Seq("2.0DHA", "2.1DHA", "3.1A", "6.2", "2.1DHS", "6.1B", "6.1A", "2.1A", "2.0DHS", "3.0A", "6.3", "2.0A", "6.4")

  def main(args: Array[String]) = {


    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val dq = objectMapper.readValue[DataQuality](Common.dqStr)



    var sparkBuilder = SparkSession
      .builder()
      //.master("local[*]")
      .appName(s"DataQuality-Simple-Experimental")
      // .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      .config("spark.mongodb.output.uri", s"mongodb://10.141.10.121:27017/dataquality.results")

    val spark = sparkBuilder.getOrCreate()

    val dqFunction = (row: Row) => {
      val dqin = SparkRowUtils.fromRowToMap(row)
      dq.getDqout(dqin)
    }

    val executeDQUdf = udf((row: Row) => {
      dqFunction(row)
    })

    val dqin = Seq("ubicacionProvincia", "tarifa", "propiedadICPTitular", "potenciaContratada").map(col(_))
    // TODO OJO: siempre va a devolver las cuatro métricas.
    //val dqout = Seq("dataQuality", "accuracy", "completness", "credibility", "consistency").zipWithIndex.map(x => col("tempdqout").getItem(x._2).as(x._1))

    //val ds = spark.read.json("/home/alvaro/datasets/hidrocantabrico_split.json")
    //val ds = spark.read.schema(Common.deserializeSchema(Common.schemaStr)).json("hdfs://10.141.10.111:8020/user/snape/cbd/hidrocantabrico.json")
    val ds = spark.read.schema(Common.deserializeSchema(Common.schemaStr)).json("hdfs://10.141.10.125:8020/datasets/hidrocantabrico_split.json")
    //val hidrocantabrico = spark.read.json("/home/alvaro/datasets/endesa_datos_agregados_split.json")
    //  .map()
    .withColumn("dqout", explode(array(executeDQUdf(struct(dqin: _*))))) // TODO Bastaría con esta column
    //  .withColumn("dqout.dataQuality", col("dqout.dataQuality"))
    //  .withColumn("dqout.accuracy", col("dqout.accuracy"))
    //  .withColumn("dqout.completness", col("dqout.completeness"))
    //  .withColumn("dqout.credibility", col("dqout.credibility"))
    //  .withColumn("dqout.consistency", col("dqout.consistency"))
    //    .withColumn("dqout", struct(dqout: _*))

    //ds.printSchema
    //ds.show(500)
    //ds.select("ubicacionProvincia").distinct.show
    MongoSpark.save(ds)
  }
}
