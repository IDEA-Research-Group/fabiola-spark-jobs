package es.us.idea.titan

import es.us.idea.dsl
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse

case class WithColumnSQLRecipe(withColumn: String, expr: String)
case class FABIOLARecipe(datasetPath: String,
                         outDatasetPath: String,
                         //datasetFormat: String, // Can be parquet, json, csv
                         copPath: String,
                         preCopTransformations: Seq[WithColumnSQLRecipe],
                         in: Seq[String],
                         out: Seq[String],
                         other: Seq[String],
                         postCopTransformations: Seq[WithColumnSQLRecipe]
                 )

object FABIOLARecipe {

  implicit val formats: Formats = DefaultFormats

  def fromJson(path: String) : FABIOLARecipe = {
    val str = scala.io.Source.fromFile(path).mkString
    val json = parse(str)
    json.extract[FABIOLARecipe]
  }

  def applyRecipe(df: DataFrame, recipe: FABIOLARecipe): DataFrame = {
    import dsl.implicits._

    val preparedDataset =
      recipe.preCopTransformations.foldLeft(df)((df, wcr) => df.withColumn(wcr.withColumn, expr(wcr.expr)))

    val copDF = preparedDataset
      .cop
      .path(recipe.copPath)
      .in(recipe.in: _*)
      .out(recipe.out: _*)
      .execute()


    val postPreparationDataset =
      recipe.postCopTransformations.foldLeft(copDF)((df, wcr) => df.withColumn(wcr.withColumn, expr(wcr.expr)))

    postPreparationDataset
      .select(("in" +: "out" +: recipe.other).map(col): _*)
  }

}