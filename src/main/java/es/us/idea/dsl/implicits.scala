package es.us.idea.dsl

import org.apache.spark.sql.DataFrame

object implicits {

  implicit class COP(df: DataFrame) {
    def cop: From = new From(df)
  }

}
