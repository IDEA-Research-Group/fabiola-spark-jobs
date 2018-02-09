package es.us.idea

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.mongodb.spark.MongoSpark
import es.us.idea.cop._
import es.us.idea.utils.{MongoDB, SparkRowUtils, Utils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bson.BasicBSONObject

import collection.JavaConversions._
import scala.util.Try

object Main {
  def main(args: Array[String]) = {

    val instanceId = args.head
    //val instanceId = "202"
    val datasetUri = args(1)
    //val datasetUri = "/home/alvaro/datasets/ENDESA/formateado/endesa_datos_agregados_split.json"
    val mongoOutputUri = args(2)
    //val mongoOutputUri = "mongodb://localhost:27017/test.results"

    val model = args(3)
    //val model = "endesa"

    val spark = SparkSession
      .builder()
      .appName("FabiolaJob_"+instanceId)
      //.master("local[*]")
      .master("spark://debian:7077")
      //.config("spark.mongodb.input.uri","mongodb://10.141.10.111:27017/fabiola.results")
      //.config("spark.mongodb.input.readPreference.name","secondaryPreferred")
      //.config("spark.mongodb.output.uri","mongodb://localhost:27017/test.results")
      .config("spark.mongodb.output.uri", mongoOutputUri)
      .config("spark.blockManager.port", 38000)
      .config("spark.broadcast.port", 38001)
      .config("spark.driver.port", 38002)
      .config("spark.executor.port", 38003)
      .config("spark.fileserver.port", 38004)
      .config("spark.replClassServer.port", 38005)
      .config("spark.network.timeout", "240s")
      .config("spark.executor.heartbeatInterval", "60s")
      .config("spark.files.fetchTimeout", "240s")

      .getOrCreate()

    import spark.implicits._

    val copDefinition = ModelDefinition(
      """
        |    val consumoActual = in.get("consumo").get.asInstanceOf[List[Map[String, Any]]]
        |    //println(consumoActual)
        |    //val model = new Model("ElectricityCOP")
        |    val precioPotencia = Array(16, 10, 2)
      """.stripMargin,
      """
        |    val TP = model.intVarArray("gasto mensual", consumoActual.length, 0, IntVar.MAX_INT_BOUND)
        |    val TPTotal = model.intVar("Coste total", 0, IntVar.MAX_INT_BOUND)
        |    val potenciaContratada = model.intVarArray("Potencias contratadas", 3, 0, IntVar.MAX_INT_BOUND)
        |
        |    val potenciaFactura = model.intVarMatrix("Potencia Factura", consumoActual.length, 3, 0, IntVar.MAX_INT_BOUND)
        |    val terminoPotencia = model.intVarMatrix("Termino Potencia", consumoActual.length, 3, 0, IntVar.MAX_INT_BOUND)
        |
        |    potenciaContratada(0) = model.intVar("Potencia Contratada 1", 0, 500)
        |    potenciaContratada(1) = model.intVar("Potencia Contratada 2", 0, 500)
        |    potenciaContratada(2) = model.intVar("Potencia Contratada 3", 0, 500)
      """.stripMargin,
      """
        |    val cien = model.intVar("Cien", 100, 100)
        |    for (i <- 0 until consumoActual.length) {
        |      for (j <- 0 until 3) {
        |        val aux = consumoActual(0).get("potencias").get.asInstanceOf[Map[String, Any]].get("p1").get.asInstanceOf[Double].toInt
        |
        |        model.ifThen(
        |          model.arithm(model.intScaleView(potenciaContratada(j), 85), ">", aux * 100),
        |          model.div(model.intScaleView(potenciaContratada(j), 85), cien, potenciaFactura(i)(j))
        |        )
        |
        |        model.ifThen(model.arithm(model.intScaleView(potenciaContratada(j), 105), "<", aux * 100),
        |          model.div(model.intOffsetView(model.intMinusView(model.intScaleView(potenciaContratada(j), 210)), aux * 300), cien, potenciaFactura(i)(j))
        |        )
        |
        |        val c3 = model.and(model.arithm(model.intScaleView(potenciaContratada(j), 85), "<=", aux * 100), model.arithm(model.intScaleView(potenciaContratada(j), 105), ">=", aux * 100))
        |        model.ifThen(c3, model.arithm(potenciaFactura(i)(j), "=", aux))
        |
        |        model.times(potenciaFactura(i)(j), precioPotencia(j) * 30, terminoPotencia(i)(j)).post()
        |      }
        |    }
        |
        |    // Calculo de TPi
        |    for (i <- 0 until TP.length)
        |      model.sum(terminoPotencia(i), "=", TP(i)).post()
        |
      """.stripMargin,
      """
        |    model.sum(TP, "=", TPTotal).post()
        |    model.setObjective(Model.MINIMIZE, TPTotal)
      """.stripMargin,
      """
        |    val solver = model.getSolver
        |    val solution = solver.findOptimalSolution(TPTotal, false)
        |
        |    val res = Map("p1" -> solution.getIntVal(potenciaContratada(0)), "p2" -> solution.getIntVal(potenciaContratada(1)), "p3" -> solution.getIntVal(potenciaContratada(2)))
        |    println("Solucion: " + res + "\n ***** Optimo: "+ solution.getIntVal(TPTotal) +"\n ***** Problema: "+in)
        |    return res
      """.stripMargin
    )

    val modelBuilder = new ModelBuilder("ElectricityCOP", copDefinition)
    val classStr = modelBuilder.buildClass
    //ClassCompiler.loadClass(classStr)

    model match {
      case "hidrocantabrico" =>
        val rdd = spark.sparkContext.textFile(datasetUri)
          .map(x => Utils.jsonToMap(x))
          .map(x => x++COPElectricidad.executeCop(x)++Map("instanceId" -> instanceId))
          .map(x => x++calculateOptimization(x, "totalFacturaActual"))
          .map(m => JSON.parse(Utils.mapToJson(m)).asInstanceOf[DBObject])
        MongoSpark.save(rdd)
      case "conquense" =>
        val rdd = spark.sparkContext.textFile(datasetUri)
          .map(x => Utils.jsonToMap(x))
          .map(x => x++COPElectricidadConquense.executeCop(x)++Map("instanceId" -> instanceId))
          .map(x => x++calculateOptimization(x, "totalFacturaActual"))
          .map(m => JSON.parse(Utils.mapToJson(m)).asInstanceOf[DBObject])
        MongoSpark.save(rdd)
      case "endesa" =>
        val rdd = spark.sparkContext.textFile(datasetUri)
          .map(x => Utils.jsonToMap(x))
          .map(x => x++COPElectricidadEndesa.executeCop(x)++Map("instanceId" -> instanceId))
          //.map(x => x++calculateOptimization(x, "totalFacturaActual"))
          .map(m => JSON.parse(Utils.mapToJson(m)).asInstanceOf[DBObject])
        MongoSpark.save(rdd)
    }


    /*
    def columnExists(dataset: Dataset[_], column:String) = Try(dataset(column)).isSuccess

    var dataset =
      spark.read.json(datasetUri)

        if(columnExists(dataset, "_corrupt_record"))
          dataset = dataset.filter("_corrupt_record is null").drop("_corrupt_record")

    val rdd = dataset
      .rdd
      .map(x => SparkRowUtils.fromRowToMap(x))
      .map(x => x++COPElectricidad.executeCop(x)++Map("instanceId" -> instanceId))
      .map(x => x++calculateOptimization(x, "totalFacturaActual"))
      .map(m => JSON.parse(Utils.mapToJson(m)).asInstanceOf[DBObject])
*/


    spark.close()
  }

  def calculateOptimization(in: Map[String, Any], currentObjectiveValue: String): Map[String, Any] = {
    if(in.exists(_._1 == "optimal") && in.exists(_._1 == currentObjectiveValue)){
      val current = in.get(currentObjectiveValue).get.asInstanceOf[Double]
      val optimal = in.get("optimal").get.asInstanceOf[Double]
      Map("optimization" -> ((optimal-current)/current) * 100.0)
    }else{
      Map("optimization" -> 0.0)
    }


  }
}
