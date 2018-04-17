package es.us.idea

import java.lang.reflect.InvocationTargetException

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import com.mongodb.spark.sql.helpers.StructFields
import es.us.idea.cop._
import es.us.idea.cop.definitions.ModelDefinitions
import es.us.idea.exceptions._
import es.us.idea.exceptions.datasource._
import es.us.idea.listeners.SparkListenerShared
import es.us.idea.utils.{Datasources, FabiolaDatabase, Utils}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import scala.collection.JavaConverters._
import scala.tools.reflect.ToolBoxError

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
    //val fabiolaDBUri = args(0)
    //val fabiolaDBName = args(1)
    //val instanceId = args(2)

    // Only for development purposes
    val fabiolaDBUri= "mongodb://estigia.lsi.us.es:12527"
    //val fabiolaDBUri= "mongodb://10.141.10.125:27017"
    val fabiolaDBName = "fabiola"
    val instanceId = "5ad47fc955714bd8acf76256"

    /** Connect to MongoDB and get the Instance, ModelDefinition and Dataset for this instance
      */
    val fabiolaDatabase = new FabiolaDatabase(fabiolaDBUri, fabiolaDBName)
    val instance = fabiolaDatabase.getInstance(instanceId)
    val modelDefinition = fabiolaDatabase.getModelDefinition(instance.modelDefinition.toString)
    val dataset = fabiolaDatabase.getDataset(instance.dataset.toString)

    /**
      * Configure the SparkListener shared variables
      */
    SparkListenerShared.setInstanceId(instanceId)
    SparkListenerShared.setFabiolaDatabase(fabiolaDatabase)

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

    /** Create the SparkSession object
      */
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Fabiola-COPJob_${instanceId}")
      .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      .config("spark.mongodb.output.uri", s"${Utils.removeLastSlashes(fabiolaDBUri)}/$fabiolaDBName.results")
      .getOrCreate()

    try {
      /** Generate the model class string
        */
      val modelBuilder = new ModelBuilder(modelDefinition)
      val classStr = modelBuilder.buildClass

      /** Create the User Defined Functions
        *
        */
      val executeCopUdf = udf((row: Row) => {
        ClassCompiler.callMethod(classStr, row, timeout)
      })
      val toObjectId = udf(() => {
        ObjectId(instanceId)
      })

      /** Create the datasource and get the dataset
        */
      val datasource = new Datasources(spark, dataset)
      var ds = datasource.getDataset

      /**
        * Apply the COP
        */
      ds = ds
        .withColumn("modelOutput", explode(array(executeCopUdf(struct(in: _*)))))
        .withColumn("instanceId", toObjectId())
        .withColumn("in", struct(in: _*))
        .withColumn("out", struct(out: _*))

      if (other.nonEmpty) ds = ds.withColumn("ot", struct(other: _*))
      if (includeMetrics) ds = ds.withColumn("metrics", struct(metrics: _*))

      ds = ds.select(selectCols: _*)

      MongoSpark.save(ds)

      SparkListenerShared.setHasSuccessfullyFinished
    } catch {
      case e: UnsupportedFormatException => SparkListenerShared.setErrorMsg("Unsupported format")
      case e: UnsupportedDatasourceException => SparkListenerShared.setErrorMsg("Unsupported datasource")
      case e: IllegalDatasourceConfigurationException => SparkListenerShared.setErrorMsg("Bad datasource configuration")
      case e: ErrorConnectingToDatasource => SparkListenerShared.setErrorMsg("Error connecting to datasource")
      case e: PathNotFoundException => SparkListenerShared.setErrorMsg("Path not found")
      case e: AnalysisException => SparkListenerShared.setErrorMsg(e.getMessage) //Thrown if some column name doesn't exist
      case e: SparkException => { // These exception are expected to be thrown if they happen inside the COP definition
        val t = ExceptionUtils.getRootCause(e)
        t match {
          case cce: ClassCastException => SparkListenerShared.setErrorMsg(t.getMessage)
          case cce: ToolBoxError => SparkListenerShared.setErrorMsg(t.getMessage)
          case _ => {
            // Check if one of the root causes is InvocationTargetException. That may be caused by an exception thrown
            // in the Model Definition
            val iteOpt = ExceptionUtils.getThrowableList(e).asScala
              .filter(x => x.getClass equals classOf[InvocationTargetException]).headOption
            if(iteOpt.isDefined){
              SparkListenerShared.setErrorMsg(s"Exception thrown from ModelDefinition: ${t}")
            }
          }
        }
      }
    } finally {
      spark.close()
    }
  }
}
