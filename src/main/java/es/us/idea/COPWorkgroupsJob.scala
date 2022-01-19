package es.us.idea

import java.lang.reflect.InvocationTargetException

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.sql.fieldTypes.ObjectId
import es.us.idea.cop.definitions.ModelDefinitions
import es.us.idea.cop._
import es.us.idea.exceptions.datasource._
import es.us.idea.listeners.SparkListenerShared
import es.us.idea.utils.{Datasources, FabiolaDatabase, SparkRowUtils, Utils}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.{HashPartitioner, SparkException}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

import scala.tools.reflect.ToolBoxError


object COPWorkgroupsJob {

  def main(args: Array[String]) = {
    //val modelBuilder = new ModelBuilder(COPModels.conquenseDef)
    //val classStr = modelBuilder.buildClass

    //val executeCopUdf = udf((row: Row) => {
    //  //ClassCompiler.callMethod(classStr, row)
    //  COPElectricidadConquense.executeCop(SparkRowUtils.fromRowToMap(row))
    //})

    val rowIndexToPartitionId = udf( (i: Int) => i/10)

    //val path = "/home/alvaro/datasets/hidrocantabrico_split.json"
    val path = "hdfs://10.141.10.111:8020/user/snape/cbd/hidrocantabrico.json"

    var sparkBuilder = SparkSession
      .builder()
      //.master("local[1]")
      .appName(s"Fabiola-Workgroups")
      //.config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      .config("spark.mongodb.output.uri", "mongodb://10.141.10.125:27017/fabiola.workgroups") // TODO añadir

    val spark = sparkBuilder.getOrCreate()

    val in = Seq("consumo", "precioTarifa").map(col(_))

    val ds = spark.read.json(path)
      .sort("totalFacturaActual") // TODO este es el criterio de ordenación
    ds.printSchema

    //ds.printSchema()

    val dsSchema = ds.schema
      .add("partitionId", "Int")

    val updatedDsSchema = dsSchema
      .add("out_consumo", "Double", true)
      .add("out_p1", "Double", true)
      .add("out_p2", "Double", true)
      .add("out_p3", "Double", true)
      .add("out_optimization", DoubleType, true)

    val optRow: Option[Row] = None
    val initialUpperBound = 10000.0
    val initialTuple = (initialUpperBound, optRow)

    val dsSize = ds.count.toInt
    val partitionSize = 10
    val nPartitions = math.ceil(dsSize.toDouble / partitionSize).toInt
    //println(nPartitions)
    //println(dsSize)

    // val np = ds
    //   .rdd
    //   .zipWithIndex()
    //   .map(x => (x._2 / partitionSize, x._1))
    //   .partitionBy(new HashPartitioner(dsSize / partitionSize))
    //   .mapPartitions( iterator => Iterator(iterator.length))

    val np = ds
      .rdd
      .zipWithIndex()
      .map(x => (x._2 / partitionSize, x._1))
      .partitionBy(new HashPartitioner(nPartitions))
      .map(x => Row.fromSeq(x._2.toSeq ++ Seq[Any](x._1.toInt)) )
      .mapPartitions(iterator => {
         val workgroup = iterator.map(x => (0.0, Option(x))).toSeq

         workgroup.scanLeft(initialTuple)((previous, current) => {
           if(current._2.isDefined){
             val thisRow: Row = new GenericRowWithSchema(current._2.get.toSeq.toArray, dsSchema)

             val out = COPElectricidad.executeCop(SparkRowUtils.fromRowToMap(thisRow), previous._1.toInt)
             val newOptimal = out.out.head
             val oldOptimal = thisRow.getDouble(thisRow.fieldIndex("totalFacturaActual"))

             val increment: Double = if(newOptimal >= 0) (newOptimal - oldOptimal) / oldOptimal else Double.MaxValue

             ( if(out.out(0) < 0.0) previous._1 else out.out(0), Option(Row.fromSeq(current._2.get.toSeq ++ Seq[Any]( out.out.head.toDouble, out.out(1).toDouble, out.out(2).toDouble, out.out(3).toDouble, increment ))) )
           } else current
         })
           .filter(_._2.isDefined)
           .iterator
      })
      .map(x => x._2.get)
      //.map(x => x)
      //.mapPartitions( iterator => Iterator(iterator.map(_._1).toSeq.mkString(", ") + " *** "  ) )
    // import spark.implicits._



    val df = spark.createDataFrame(np, updatedDsSchema)/*.mapPartitions((iterator) => iterator).*/
    //df.show(1000)
    MongoSpark.save(df)


    //println(np.count)
    //println(np.collect.toSeq)


    //zipWithIndex(ds)
    //  .withColumn("partitionId", rowIndexToPartitionId(col("index")))
    //    .partition
    //  .show(1000)

    spark.close()

  }

  // https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex/37088030
  def zipWithIndex(df: DataFrame, offset: Long = 1, indexName: String = "index") = {
    val dfWithPartitionId = df.withColumn("partition_id", spark_partition_id()).withColumn("inc_id", monotonically_increasing_id())

    val partitionOffsets = dfWithPartitionId
      .groupBy("partition_id")
      .agg(count(lit(1)) as "cnt", first("inc_id") as "inc_id")
      .orderBy("partition_id")
      .select(sum("cnt").over(Window.orderBy("partition_id")) - col("cnt") - col("inc_id") + lit(offset) as "cnt" )
      .collect()
      .map(_.getLong(0))
      .toArray

    dfWithPartitionId
      .withColumn("partition_offset", udf((partitionId: Int) => partitionOffsets(partitionId), LongType)(col("partition_id")))
      .withColumn(indexName, col("partition_offset") + col("inc_id"))
      .drop("partition_id", "partition_offset", "inc_id")
  }
}
