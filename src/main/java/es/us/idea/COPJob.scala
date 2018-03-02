package es.us.idea

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import com.mongodb.spark.sql.helpers.StructFields
import es.us.idea.cop._
import es.us.idea.cop.definitions.ModelDefinitions
import es.us.idea.utils.{FabiolaDatabase, Utils}
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes}

/** COPJob
  * This job is intended to get the instance configuration and the COP Definition Model from the Fabiola MongoDB
  * database, compile the COP Definition Model, and execute it through a Spark cluster.
  */
object COPJob {
  /** The main function must receive three parameters:
    * - The MongoDB database used by Fabiola
    * - The name of the database
    * - The instanceId of this problem instance
    */
  def main(args: Array[String]) = {
    val fabiolaDBUri = args(0)
    val fabiolaDBName = args(1)
    val instanceId = args(2)

    // Only for development purposes
    //val fabiolaDBUri= "mongodb://localhost:27017"
    //val fabiolaDBName = "test"
    //val instanceId = "5a990fca43a3b63cdbc759ef"

    /** Connect to MongoDB and get the Instance and ModelDefinition for this instance
      */
    val fabiolaDatabase = new FabiolaDatabase(fabiolaDBUri, fabiolaDBName)
    val instance = fabiolaDatabase.getInstance(instanceId)
    val modelDefinition = fabiolaDatabase.getModelDefinition(instance.modelDefinition.toString)

    /** Generate the model class string
      */
    val modelBuilder = new ModelBuilder(modelDefinition)
    val classStr = modelBuilder.buildClass

    /** Get the instance configuration
      */
    val includeMetrics = instance.metrics

    val in = instance.in.map(col(_))
    val out = instance.out.zipWithIndex.map(x => col("modelOutput.out").getItem(x._2).as(x._1))
    val metrics = Seq("solvingTime", "buildingTime", "totalTime", "variableCount", "constraintCount")
      .zipWithIndex.map(x => col("modelOutput.metrics").getItem(x._2).as(x._1))
    val other = instance.ot.map(col(_))

    var selectCols = Seq(column("instanceId"), column("in"), column("out"))

    if (other.nonEmpty) selectCols = selectCols :+ column("ot")
    if (includeMetrics) selectCols = selectCols :+ column("metrics")

    val timeout = instance.timeout

    /** Create the User Defined Functions
      *
      */
    val executeCopUdf = udf((row: Row) => {
      ClassCompiler.callMethod(classStr, row, timeout)
    })
    val toObjectId = udf(() => {
      ObjectId(instanceId)
    })


    /** Create the SparkSession object
      */
    val spark = SparkSession
      .builder()
      //.master("local[*]")
      .appName(s"Fabiola-COPJob_${instanceId}")
      //.config("spark.mongodb.input.readPreference.name","secondaryPreferred")
      .config("spark.mongodb.output.uri", s"${Utils.removeLastSlashes(fabiolaDBUri)}/$fabiolaDBName.results")
      .getOrCreate()

    var ds = spark.read.json(instance.datasetUri)
      .withColumn("modelOutput", explode(array(executeCopUdf(struct(in: _*)))))
      .withColumn("instanceId", toObjectId())
      .withColumn("in", struct(in: _*))
      .withColumn("out", struct(out: _*))

    if (other.nonEmpty) ds = ds.withColumn("ot", struct(other: _*))
    if (includeMetrics) ds = ds.withColumn("metrics", struct(metrics: _*))

    ds = ds.select(selectCols: _*)

    ds.printSchema
    MongoSpark.save(ds)

    spark.close()
  }
}
