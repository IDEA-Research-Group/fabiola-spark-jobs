package es.us.idea.utils

import java.io.{EOFException, IOException}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import es.us.idea.exceptions._
import es.us.idea.exceptions.datasource._
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.mongodb.scala.MongoException

class Datasources(spark: SparkSession, dataset: es.us.idea.dao.Dataset) {

  def loadFromMongo(): Dataset[_] = {
    val collection = dataset.path.split('.').last
    val auth = if (dataset.credentials.isDefined) s"${dataset.credentials.get.user}:${dataset.credentials.get.password}@" else ""
    try {
      val readConfig = ReadConfig(
        Map(
          "uri" -> s"mongodb://${auth}${dataset.hostname}:${dataset.port}/${dataset.path}",
          //"uri" -> s"mongodb://${dataset.port}/${dataset.path}",
          "collection" -> collection,
          "readPreference.name" -> "secondaryPreferred"
        )
      )
      MongoSpark.loadAndInferSchema(spark, readConfig)
    } catch {
      case iae: IllegalArgumentException => throw new IllegalDatasourceConfigurationException("Bad datasource configuration")
      case mse: MongoException => throw new ErrorConnectingToDatasource("Error connecting to datasource")
    }
  }

  def loadFromHdfs(): Dataset[_] = {
    if (!dataset.format.isDefined) throw new UnsupportedFormatException("Format must be specified")
    val format = dataset.format.get
    if (!List("json", "csv").contains(format)) throw new UnsupportedFormatException("Format not supported")
    try {
      if (format equals "json")
        spark.read.json(s"hdfs://${dataset.hostname}:${dataset.port}/${dataset.path}")
        //spark.read.json(s"${dataset.path}/fdsdfs")
      else
        spark.read.csv(s"hdfs://${dataset.hostname}:${dataset.port}/${dataset.path}")
    } catch {
      case iae: IllegalArgumentException => throw new IllegalDatasourceConfigurationException("Bad datasource configuration")
      case ioe: IOException => throw new IllegalDatasourceConfigurationException("Bad datasource configuration")
      case efe: EOFException => throw new ErrorConnectingToDatasource("Error connecting to datasource")
      case ae: AnalysisException => throw new PathNotFoundException("Specified path not found")
      case pnfe: org.apache.hadoop.fs.PathNotFoundException => throw new PathNotFoundException("Specified path not found")
    }

  }

  def getDataset(): Dataset[_] = {
    dataset.datasource match {
      case "mongo" => loadFromMongo()
      case "hdfs" => loadFromHdfs()
      case _ => throw new UnsupportedDatasourceException("Datasource not supported")
    }
  }
}
