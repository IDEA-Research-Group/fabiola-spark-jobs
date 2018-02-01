package es.us.idea.utils

import com.mongodb.DBObject
import com.mongodb.spark.MongoSpark
import com.mongodb.util.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object MongoDB {
  def saveMapRdd(rdd: RDD[Map[String, Any]]) = {
    MongoSpark.save(rdd.map(m => JSON.parse(Utils.mapToJson(m)).asInstanceOf[DBObject]))
  }
}
