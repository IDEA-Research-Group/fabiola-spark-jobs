package es.us.idea

import es.us.idea.cop.definitions.ModelDefinitions
import es.us.idea.cop.{ClassCompiler, ModelBuilder}
import es.us.idea.dao.COPModel
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestDSL extends App {


  //val copModel = COPModel("name", "definition")
  val copModel = ModelDefinitions.conquenseDef

  var spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import dsl.implicits._

  val df = spark.read.json("input/conquense_output_split.json")
  df.show()
  df.printSchema()

  val copDf = df.cop
    .string(copModel.model)
    .in("consumos", "precio_potencia")
    .out("TPTotal", "pc1", "pc2", "pc3")
    .execute()

  copDf.show()
  copDf.printSchema()


}
