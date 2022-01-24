package es.us.idea

import es.us.idea.Test.copModel
import es.us.idea.cop.{ClassCompiler, ModelBuilder}
import es.us.idea.cop.definitions.ModelDefinitions
import es.us.idea.dao.COPModel
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, column, explode, struct, udf}

object Test extends App {


  //val copModel = COPModel("name", "definition")
  val copModel = ModelDefinitions.conquenseDef


  /** Generate the model class string
   */
  val modelBuilder = new ModelBuilder(copModel)
  val classStr = modelBuilder.buildClass

  val executeCopUdf = udf((row: Row) => {
    ClassCompiler.callMethod(classStr, row)
  })


  var spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.json("input/conquense_output_split.json")
  df.show()
  df.printSchema()

  val copdf = applyCop(df, Seq("consumos", "precio_potencia"), Seq("TPTotal", "pc1", "pc2", "pc3"), generateCopModelUdf(ModelDefinitions.conquenseDef))

  copdf.show()
  copdf.printSchema()


  def generateCopModelUdf(copModel: COPModel): UserDefinedFunction = {
    val modelBuilder = new ModelBuilder(copModel)
    val classStr = modelBuilder.buildClass
    udf((row: Row) => {
      ClassCompiler.callMethod(classStr, row)
    })
  }

  def applyCop(df: DataFrame, in: Seq[String], out: Seq[String], executeCopUdf: UserDefinedFunction): DataFrame = {
    val inCols = in.map(col)
    val outCols = out.zipWithIndex.map(x => col("modelOutput.out").getItem(x._2).as(x._1))
    df.withColumn("modelOutput", explode(array(executeCopUdf(struct(inCols: _*)))))
      .withColumn("in", struct(inCols: _*))
      .withColumn("out", struct(outCols: _*))
  }


}
