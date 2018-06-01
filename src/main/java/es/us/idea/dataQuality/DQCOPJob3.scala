package es.us.idea.dataQuality

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mongodb.spark.MongoSpark
import es.us.idea.cop.definitions.COPModels
import es.us.idea.cop.{ClassCompiler, ModelBuilder}
import es.us.idea.dataQuality.internal.DataQuality
import es.us.idea.utils.SparkRowUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object DQCOPJob3 {
  def main(args: Array[String]) = {

    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val dq = objectMapper.readValue[DataQuality](Common.dqStr)


    // Clase del COP
    val modelBuilder = new ModelBuilder(COPModels.hidrocantabricoDef)
    val classStr = modelBuilder.buildClass

    var sparkBuilder = SparkSession
      .builder()
      //.master("local[*]")
      .appName(s"DataQuality-DQCOPJob3-Experimental")
      // .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      .config("spark.mongodb.output.uri", s"mongodb://10.141.10.121:27017/dataquality.results")

    val spark = sparkBuilder.getOrCreate()

    val dqFunction = (row: Row) => {
      val dqin = SparkRowUtils.fromRowToMap(row)
      dq.getDqout(dqin)
    }

    val executeDQUdf = udf((row: Row) => {
      dqFunction(row)
    })

    val executeCOPUdf = udf((row: Row) => {
      ClassCompiler.callMethod(classStr, row)
    })

    val dqin = Seq("ubicacionProvincia", "tarifa", "propiedadICPTitular", "potenciaContratada", "consumo", "precioTarifa").map(col(_))
    // TODO OJO: siempre va a devolver las cuatro métricas.
    //val dqout = Seq("dataQuality", "accuracy", "completness", "credibility", "consistency").zipWithIndex.map(x => col("tempdqout").getItem(x._2).as(x._1))

    //val ds = spark.read.json("/home/alvaro/datasets/hidrocantabrico_split.json")
    val ds = spark.read.schema(Common.deserializeSchema(Common.schemaStr)).json("hdfs://10.141.10.111:8020/user/snape/cbd/hidrocantabrico.json")
      //val hidrocantabrico = spark.read.json("/home/alvaro/datasets/endesa_datos_agregados_split.json")
      //  .map()
      .withColumn("dqout", explode(array(executeDQUdf(struct(dqin: _*))))) // TODO Bastaría con esta column
      .withColumn("out", explode(array(executeCOPUdf(struct(dqin: _*)))))
    //  .withColumn("dqout.dataQuality", col("dqout.dataQuality"))
    //  .withColumn("dqout.accuracy", col("dqout.accuracy"))
    //  .withColumn("dqout.completness", col("dqout.completeness"))
    //  .withColumn("dqout.credibility", col("dqout.credibility"))
    //  .withColumn("dqout.consistency", col("dqout.consistency"))
    //    .withColumn("dqout", struct(dqout: _*))

    //ds.printSchema
    //ds.show(500)
    //ds.select("ubicacionProvincia").distinct.show
    MongoSpark.save(ds)
  }
}
