package es.us.idea.dsl

import es.us.idea.dao.COPModel
import org.apache.spark.sql.DataFrame

class From(df: DataFrame) {

  def string(modelDefinition: String) = {
    new COPSparkEngine(df, new COPModel("_", modelDefinition))
  }

}
