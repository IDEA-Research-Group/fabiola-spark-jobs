package es.us.idea

import es.us.idea.utils.{Datasources, FabiolaDatabase, Statuses}
import es.us.idea.exceptions._
import es.us.idea.listeners.SparkListenerShared
import org.apache.spark.sql.SparkSession

object DatasetUtilityJob {
  def main(args: Array[String]) = {
    //val fabiolaDBUri = args(0)
    //val fabiolaDBName = args(1)
    //val datasetId = args(2)

    // Only for development purposes
    val fabiolaDBUri = "mongodb://estigia.lsi.us.es:12527"
    val fabiolaDBName = "fabiola"
    //val datasetId = "5acdce19a53b5093dd09ecc1" // mongodb
    //val datasetId = "5acf1b32a53b5093dd09ee63"   // hdfs json (does not work)
    val datasetId = "5acf2976a53b5093dd09ee64" // local json

    val fabiolaDatabase = new FabiolaDatabase(fabiolaDBUri, fabiolaDBName)
    val dataset = fabiolaDatabase.getDataset(datasetId)

    SparkListenerShared.setDatasetId(datasetId)
    SparkListenerShared.setFabiolaDatabase(fabiolaDatabase)

    /** Create the SparkSession object
      */
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"Fabiola-DatasetUtilityJob_${datasetId}")
      .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      .getOrCreate()

    try {
      val datasource = new Datasources(spark, dataset)

      val ds = datasource.getDataset()
      val schema = ds.schema.json
      fabiolaDatabase.updateDatasetSchema(datasetId, schema)

      SparkListenerShared.setHasSuccessfullyFinished
    } catch {
      case e: UnsupportedFormatException => SparkListenerShared.setErrorMsg("Unsupported format")
      case e: UnsupportedDatasourceException => SparkListenerShared.setErrorMsg("Unsupported datasource")
      case e: IllegalDatasourceConfigurationException => SparkListenerShared.setErrorMsg("Bad datasource configuration")
      case e: ErrorConnectingToDatasource => SparkListenerShared.setErrorMsg("Error connecting to datasource")
      case e: PathNotFoundException => SparkListenerShared.setErrorMsg("Path not found")
    } finally {

      /**
        * Close the SparkSession
        **/
      spark.close()
    }
  }

}
