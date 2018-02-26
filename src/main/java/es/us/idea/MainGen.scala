package es.us.idea

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import es.us.idea.cop._
import es.us.idea.cop.definitions.ModelDefinitions
import es.us.idea.utils.Utils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.bson.BasicBSONObject
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.bson.types.ObjectId
import org.mongodb.scala.MongoClient
import org.mongodb.scala.model.Filters._

import collection.JavaConversions._
import scala.util.Try
object MainGen {
  def main(args: Array[String]) = {

    //val instanceId = args.head
    val instanceId = "5a9413d2073e827e80be055c"
    //val partitions = args(1).toInt
    //val datasetUri = args(1)
    val datasetUri = "/home/alvaro/datasets/hidrocantabrico_split.json"

    val fabiolaDatabase = "mongodb://10.141.10.121:27017"

    val spark = SparkSession
      .builder()
      .appName("FabiolaJob_"+instanceId)
      .master("local[*]")
      .config("spark.mongodb.input.uri","mongodb://localhost:27017/test.hidrocantabrico")
      //.config("spark.mongodb.input.readPreference.name","secondaryPreferred")
      .config("spark.mongodb.output.uri","mongodb://localhost:27017/test.results")
      //.config("spark.mongodb.output.uri","mongodb://10.141.10.121:27017/fabiola.results")
      .getOrCreate()

    /** Connect to MongoDB and get the instance data and the cop model
      */
    val mongoDB = MongoClient(fabiolaDatabase).getDatabase("test")
    val instances = mongoDB.getCollection("instances")
    val modelDefinitions = mongoDB.getCollection("modelDefinitions")

    //val modelDefObj = modelDefinitions.find(equal("_id", ObjectId(instanceId))).first()

    //print(modelDefObj)

    /** Build the cop model
      */
    val copDefinition = ModelDefinitions.hidrocantabricoDef

    val modelBuilder = new ModelBuilder(copDefinition)
    val classStr = modelBuilder.buildClass
    ClassCompiler.loadClass(classStr)

    /** Apply the instance configuration
      */
    val includeMetrics = true

    val in = Seq("consumo", "precioTarifa").map(col(_))
    val out = Seq("consumo", "p1", "p2", "p3").zipWithIndex.map(x => col("modelOutput.out").getItem(x._2).as(x._1) )
    val metrics = Seq("solvingTime", "buildingTime", "totalTime", "variableCount", "constraintCount").zipWithIndex.map(x => col("modelOutput.metrics").getItem(x._2).as(x._1) )
    val other = Seq( "ubicacionProvincia", "ubicacionPoblacion").map(col(_))

    var selectCols = Seq(column("instanceId"), column("in"), column("out"))

    if (other.nonEmpty) selectCols = selectCols :+ column("ot")
    if (includeMetrics) selectCols = selectCols :+ column("metrics")

    val timeout = 5000.toLong

    import spark.implicits._

    //val executeCopUdf = udf((r: Row) => {COPElectricidadRow.executeCop(r)})
    val executeCopUdf = udf((row: Row) => { ClassCompiler.callMethod(row, timeout) })

    var ds = spark.read.json(datasetUri)
      .select(in++other : _* )
      .withColumn("modelOutput", explode(array(executeCopUdf(struct(in: _*)))))
      .withColumn("instanceId", lit(instanceId))
      .withColumn("in", struct( in: _* ))
      .withColumn("out", struct( out: _* ))

      if(other.nonEmpty) ds = ds.withColumn("ot", struct( other: _* ))
      if(includeMetrics) ds = ds.withColumn("metrics", struct( metrics: _* ))

      ds = ds.select(selectCols: _* )

    //println(ds.schema.prettyJson)

    //ds.show
    //ds.printSchema
    //ds.explain(true)
    MongoSpark.save(ds)

    //ds.explain

    spark.close()
  }
  /*
  def calculateOptimization(in: Map[String, Any], currentObjectiveValue: String): Map[String, Any] = {
    if(in.exists(_._1 == "optimal") && in.exists(_._1 == currentObjectiveValue)){
      val current = in.get(currentObjectiveValue).get.asInstanceOf[Double]
      val optimal = in.get("optimal").get.asInstanceOf[Double]
      Map("optimization" -> ((optimal-current)/current) * 100.0)
    }else{
      Map("optimization" -> 0.0)
    }
  }
  */
}
