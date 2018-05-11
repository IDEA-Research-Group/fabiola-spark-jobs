package es.us.idea.dataQuality

import java.lang.reflect.InvocationTargetException

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import com.mongodb.spark.sql.helpers.StructFields
import es.us.idea.cop._
import es.us.idea.dataQuality.internal.businessRules.basic.numeric.{FieldGreaterThanBR, FieldLessEqThanBR, FieldLessThanBR}
import es.us.idea.dataQuality.internal.businessRules.operations.{AndBRO, IfBRO, NotBRO}
import es.us.idea.dataQuality.internal.{DataQuality, businessRules}
import es.us.idea.dataQuality.internal.businessRules.basic.{FieldBetweenBR, FieldMatchesBR, FieldNotNullBR}
import es.us.idea.dataQuality.internal.dimension._
import es.us.idea.exceptions._
import es.us.idea.exceptions.datasource._
import es.us.idea.listeners.SparkListenerShared
import es.us.idea.utils.{Datasources, FabiolaDatabase, SparkRowUtils, Utils}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import scala.collection.JavaConverters._
import scala.io.Source
import scala.tools.reflect.ToolBoxError


object DQJob {
  //val provinciasEsp = Source.fromFile("/home/alvaro/datasets/provincias_es").getLines.toSeq
  val provinciasEsp = Seq("Araba", "Álava", "Albacete", "Alicante", "Alacant", "Almería", "Ávila", "Badajoz", "Balears (Illes)", "Barcelona", "Burgos", "Cáceres", "Cádiz", "Castellón", "Castelló", "Ciudad Real", "Córdoba", "Coruña (A)", "Cuenca", "Girona", "Granada", "Guadalajara", "Gipuzkoa", "Huelva", "Huesca", "Jaén", "León", "Lleida", "Rioja (La)", "Lugo", "Madrid", "Málaga", "Murcia", "Navarra", "Ourense", "Asturias", "Palencia", "Palmas (Las)", "Pontevedra", "Salamanca", "Santa Cruz de Tenerife", "Cantabria", "Segovia", "Sevilla", "Soria", "Tarragona", "Teruel", "Toledo", "Valencia", "València", "Valladolid", "Bizkaia", "Zamora", "Zaragoza", "Ceuta", "Melilla")

  //val tarifas = Source.fromFile("/home/alvaro/datasets/tarifas").getLines.toSeq
  val tarifas = Seq("2.0DHA", "2.1DHA", "3.1A", "6.2", "2.1DHS", "6.1B", "6.1A", "2.1A", "2.0DHS", "3.0A", "6.3", "2.0A", "6.4")

  def main(args: Array[String]) = {


    // Definicion del objeto de dq

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
        (0.75, new NotBRO(new FieldMatchesBR("ubicacionProvincia", Seq("Asturias")))),
        (0.25, new NotBRO(new FieldMatchesBR("propiedadICPTitular", Seq("Empresa distribuidora", null))))
      )
    )

    val dq = new DataQuality(
      accuracy = Option((0.2, accuracy)),
      consistency= Option((0.5, consistency)),
      completeness = Option((0.2, completness)),
      credibility = Option((0.1, credibility))
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
