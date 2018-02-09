package es.us.idea

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.mongodb.spark.MongoSpark
import es.us.idea.cop.{COPElectricidad, COPElectricidadRow}
import es.us.idea.utils.Utils
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.bson.BasicBSONObject

import collection.JavaConversions._
import scala.util.Try

object MainGen {
  def main(args: Array[String]) = {

//    val instanceId = args.head
    val instanceId = "202"
    //val partitions = args(1).toInt
    //val datasetUri = args(1)
    val datasetUri = "/home/alvaro/datasets/hidrocantabrico_split.json"

    val spark = SparkSession
      .builder()
      .appName("FabiolaJob_"+instanceId)
      .master("local[*]")
      //.master("spark://debian:7077")
      //.config("spark.mongodb.input.uri","mongodb://10.141.10.111:27017/fabiola.results")
      //.config("spark.mongodb.input.readPreference.name","secondaryPreferred")
      //.config("spark.mongodb.output.uri","mongodb://localhost:27017/test.results")
      //.config("spark.mongodb.output.uri","mongodb://10.141.10.111:27017/fabiola.results")
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

    def executeCopUdf(r: Row) = {COPElectricidadRow.executeCop(r)}

    val schema = StructType(Array(
      StructField("Out1", DoubleType, false)))

    val udf = spark.udf.register("execute", executeCopUdf _)

    val ds = spark.read.json(datasetUri)

     //ds.withColumn("out", udf(ds.columns.map(ds(_)): _*)).show
     //ds.withColumn("out", udf(ds.head)).show
/*
    val rdd = spark.sparkContext.textFile(datasetUri)
      .map(x => Utils.jsonToMap(x))
      .map(x => x++COPElectricidad.executeCop(x)++Map("instanceId" -> instanceId))
      .map(x => x++calculateOptimization(x, "totalFacturaActual"))
      .map(m => JSON.parse(Utils.mapToJson(m)).asInstanceOf[DBObject])
*/
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
    //MongoSpark.save(rdd)

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
