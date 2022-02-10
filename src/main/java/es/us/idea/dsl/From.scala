package es.us.idea.dsl

import es.us.idea.dao.COPModel
import es.us.idea.readers.{HDFSReader, PathReader, URLReader}
import es.us.idea.readers.exception.FailedToReadException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

class From(df: DataFrame) {

  implicit def str2COPSparkEngine(str: String) =
    new COPSparkEngine(df, new COPModel("_", str))

  implicit def eitherResolutor(ethr: Either[FailedToReadException, String]) =
    ethr match {
      case Right(value) => str2COPSparkEngine(value)
      case Left(value) => throw value
    }

  def string(modelDefinition: String): COPSparkEngine = modelDefinition

  def path(path: String): COPSparkEngine = new PathReader(path).getString()

  def url(url: String): COPSparkEngine = new URLReader(url).getString()

  def hdfs(uri: String, configuration: Configuration = new Configuration()): COPSparkEngine =
    new HDFSReader(uri, configuration).getString()

}
