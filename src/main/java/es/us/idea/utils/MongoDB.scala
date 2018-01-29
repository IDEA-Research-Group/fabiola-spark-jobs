package es.us.idea.utils

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SQLContext}

object MongoDB {
  def write(dataframe: DataFrame, sqlContext: SQLContext, collection: String) = {

    MongoSpark.save(
      dataframe.write.option("collection", collection).mode("append")
    )
    /*
        MongoSpark.save(
          dataframe,
          WriteConfig(Map("spark.mongodb.output.uri" -> "mongodb://10.141.10.111:27017/fabiola.results")
            ))*/
  }
}
