package es.us.idea.titan

import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  val recipe = FABIOLARecipe.fromJson("input/conquenseRecipe.json")

  val df = spark.read.json(recipe.datasetPath)

  val finalDF = FABIOLARecipe.applyRecipe(
    spark.createDataFrame(df.takeAsList(3), df.schema), recipe)

  finalDF.coalesce(1)
    .write
    .json(recipe.outDatasetPath)

}
