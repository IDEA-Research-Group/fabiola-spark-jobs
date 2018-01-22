package es.us.idea

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("FabiolaJob")
      .master("local[*]")
      //.master("spark://debian:7077")
      //.config("spark.mongodb.input.uri","mongodb://10.141.10.111:27017/fabiola.results")
      .config("spark.mongodb.input.readPreference.name","secondaryPreferred")
      //.config("spark.mongodb.output.uri","mongodb://10.141.10.111:27017/fabiola.results")
      .config("spark.blockManager.port", 38000)
      .config("spark.broadcast.port", 38001)
      .config("spark.driver.port", 38002)
      .config("spark.executor.port", 38003)
      .config("spark.fileserver.port", 38004)
      .config("spark.replClassServer.port", 38005)

      .getOrCreate()

    spark.sparkContext.parallelize(List("a", "b", "c", "a", "a", "see", "b")).countByValue().foreach(println)

    spark.close()
  }
}
