package es.us.idea.dataQuality

import java.lang.reflect.InvocationTargetException

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import com.mongodb.spark.sql.helpers.StructFields
import es.us.idea.cop._
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
import scala.tools.reflect.ToolBoxError


object DQJob {

  def main(args: Array[String]) = {
    var sparkBuilder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"DataQuality-Experimental")
      // .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      // .config("spark.mongodb.output.uri", s"${Utils.removeLastSlashes(fabiolaDBUri)}/$fabiolaDBName.results")

    val spark = sparkBuilder.getOrCreate()

    val dqFunction = (row: Row) => {
      val in = SparkRowUtils.fromRowToMap(row)
      1
      //DQFunction.numberOfMatches(in, )




    }

    val dqUdf = udf((row: Row) => {
      dqFunction(row)
    })

    val hidrocantabrico = spark.read.json("/home/alvaro/datasets/hidrocantabrico_split.json")
    //val hidrocantabrico = spark.read.json("/home/alvaro/datasets/endesa_datos_agregados_split.json")
    //  .map()

    hidrocantabrico.printSchema
    hidrocantabrico.show
    //hidrocantabrico.select("tarifa").distinct.show

  }
}
