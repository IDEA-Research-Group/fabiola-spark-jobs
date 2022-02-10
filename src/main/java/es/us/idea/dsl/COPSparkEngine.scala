package es.us.idea.dsl

import es.us.idea.cop.{ClassCompiler, ModelBuilder}
import es.us.idea.dao.COPModel
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, explode, struct, udf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

class COPSparkEngine(df: DataFrame, copModel: COPModel, in: Option[Seq[String]] = None, out: Option[Seq[String]] = None) {


  def in(in: String*) = new COPSparkEngine(df, copModel, in = Some(in), out = out)
  def out(out: String*) = new COPSparkEngine(df, copModel, in = in, out = Some(out))

  def execute() = {

    in.flatMap(ins => {
      out.map(outs => {
        val executeCopUdf = generateCopModelUdf(copModel)
        val inCols = ins.map(col)
        val outCols = outs.zipWithIndex.map(x => col("modelOutput.out").getItem(x._2).as(x._1))

        // UDFs executing twice: https://issues.apache.org/jira/browse/SPARK-17728

        val copResults = df.withColumn("modelOutput", executeCopUdf(struct(inCols: _*))).cache()
        //val copResultsRdd = copResults.cache().rdd.persist(MEMORY_AND_DISK)

        copResults
          .withColumn("in", struct(inCols: _*))
          .withColumn("out", struct(outCols: _*))
      })
    }) match {
      case Some(dataFrame) => dataFrame
      case _ => throw new NoSuchFieldException("Input and output must be set before executing the COPs. Please use in() and out() methods.")
    }

  }

  private def generateCopModelUdf(copModel: COPModel): UserDefinedFunction = {
    val modelBuilder = new ModelBuilder(copModel)
    val classStr = modelBuilder.buildClass
    udf((row: Row) => {
      ClassCompiler.callMethod(classStr, row)
    })
  }

}
