package es.us.idea

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.mongodb.spark.MongoSpark
import es.us.idea.cop.{COPElectricidad, COPElectricidadRow}
import es.us.idea.utils.Utils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.bson.BasicBSONObject
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import collection.JavaConversions._
import scala.util.Try
case class Out(out1: String, out2: String)

object MainGen {
  def main(args: Array[String]) = {

//    val instanceId = args.head
    val instanceId = "126"
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
      .config("spark.mongodb.output.uri","mongodb://localhost:27017/test.results")
      //.config("spark.mongodb.output.uri","mongodb://10.141.10.111:27017/fabiola.results")
      //.config("spark.blockManager.port", 38000)
      //.config("spark.broadcast.port", 38001)
      //.config("spark.driver.port", 38002)
      //.config("spark.executor.port", 38003)
      //.config("spark.fileserver.port", 38004)
      //.config("spark.replClassServer.port", 38005)
      //.config("spark.network.timeout", "240s")
      //.config("spark.executor.heartbeatInterval", "60s")
      //.config("spark.files.fetchTimeout", "240s")

      .getOrCreate()

    val in = Seq("consumo", "precioTarifa").map(col(_))
    val out = Seq("consumo", "p1", "p2", "p3").zipWithIndex.map(x => col("cop_out_tuple._1._"+(x._2 + 1)).as(x._1) )
    val metrics = Seq("solvingTime", "buildingTime", "totalTime", "variableCount", "constraintCount").zipWithIndex.map(x => col("cop_out_tuple._2._"+(x._2 + 1)).as(x._1) )
    val other = Seq("cups", "ubicacionProvincia", "ubicacionPoblacion").map(col(_))

    import spark.implicits._

    val executeCopUdf = udf((r: Row) => {COPElectricidadRow.executeCop(r)})

    val ds = spark.read.json(datasetUri)
      .withColumn("cop_out_tuple", executeCopUdf(struct(in: _*)))
      .withColumn("instanceId", lit(instanceId))
      .withColumn("in", struct( in: _* ))
      .withColumn("out", struct( out: _* ))
      .withColumn("other", struct( other: _*))
      .withColumn("metrics", struct( metrics: _*))
      .select("instanceId", "metrics", "in", "out", "other")

    //ds.show
    //ds.printSchema

    MongoSpark.save(ds)

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
