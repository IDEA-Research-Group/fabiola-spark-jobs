package es.us.idea

import es.us.idea.cop.definitions.ModelDefinitions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, expr, lit, udf}

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


  val calculaTPTotalUdf = udf((potenciaContratada: Seq[Double], consumos: Seq[Seq[Double]], precio: Seq[Double]) => {
    val potenciaContratadaAjustada = potenciaContratada.map(_/100.0)
    val precioAjustado = potenciaContratada.map(_/100.0)

    consumos.map(row => row.zipWithIndex.map(x => {
      val (pm, j) = (x._1/100.0, x._2)

      {
        if (pm <= 0.85 * potenciaContratadaAjustada(j)) potenciaContratadaAjustada(j) * 0.85
        else if (pm > potenciaContratadaAjustada(j) * 0.85 && pm < potenciaContratadaAjustada(j) * 1.05) pm
        else potenciaContratadaAjustada(j) + (2 * (pm - potenciaContratadaAjustada(j)))
      } * precioAjustado(j)
    }).sum).sum

  })



  val tDf = df
    .withColumn("C", expr("transform(consumos, x -> array(x.potencia_maxima_p1, x.potencia_maxima_p2, x.potencia_maxima_p3) )"))
    .withColumn("C", expr("slice(C, 1, 12)"))
    .withColumn("C", expr("transform(C, x -> transform(x, y -> cast( (y * 100) as integer) ))"))
    .withColumn("PRECIO", array(lit(25), lit(17), lit(30)))
    .select( $"localizacion.cups".as("cups"), $"C", $"PRECIO")

  tDf.show(false)
  tDf.printSchema()


  val copDf = spark.createDataFrame(tDf.takeAsList(50), tDf.schema) // TODO cambiar numero te tuplas a procesar
    .cop
    .path("input/generic.scala")
    .in("C", "PRECIO")
    .out("TPTotal", "pc1", "pc2", "pc3")
    .execute()
    .select($"cups", $"C", $"PRECIO", $"out.pc1".as("pc1"), $"out.pc2".as("pc2"), $"out.pc3".as("pc3") )
    .withColumn("TPTotal", calculaTPTotalUdf(array($"pc1", $"pc2", $"pc3"), $"C", $"PRECIO"))
    .drop("C", "PRECIO")

  copDf.coalesce(1).write.option("header", "true").csv("results")

  copDf.show(false)
  copDf.printSchema()



}
