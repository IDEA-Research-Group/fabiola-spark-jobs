package es.us.idea

import es.us.idea.cop.definitions.ModelDefinitions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, expr, lit}

object TransformSolve extends App {


  //val copModel = COPModel("name", "definition")
  val copModel = ModelDefinitions.conquenseDef

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import dsl.implicits._
  import spark.implicits._

  val df = spark.read.json("input/conquense_output_split.json")
  df.show()
  df.printSchema()

  val tDf = df
    .withColumn("C", expr("transform(consumos, x -> array(x.potencia_maxima_p1, x.potencia_maxima_p2, x.potencia_maxima_p3) )"))
    .withColumn("C", expr("slice(C, 1, 12)"))
    .withColumn("C", expr("transform(C, x -> transform(x, y -> cast( (y * 100) as integer) ))"))
    .withColumn("PRECIO", array(lit(25), lit(17), lit(30)))
    .select( $"localizacion.cups", $"C", $"PRECIO")

  tDf.show(false)
  tDf.printSchema()


  val copDf = spark.createDataFrame(tDf.takeAsList(50), tDf.schema)
    .cop
    .path("input/generic.scala")
    .in("C", "PRECIO")
    .out("TPTotal", "pc1", "pc2", "pc3")
    .execute()
    .select("out")
  copDf.show(50, false)
  copDf.printSchema()


}
