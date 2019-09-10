package es.us.idea

import java.lang.instrument.Instrumentation

import es.us.idea.cop.ClassCompiler
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

import scala.reflect.runtime.currentMirror
import scala.util.Try
import reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox
import es.us.idea.mapping.mapper.dsl.DSL._

object QuickTests {

  def main(args: Array[String]) = {

    val toolbox = scala.reflect.runtime.universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    //val code1 =
    //  q"""
    //     import es.us.idea.mapping.mapper.dsl.DSL._
    //     object Foo {
    //      create doubleMatrix "POTENCIAS" from(matrix("consumo", array("potencias.p1", "potencias.p2", "potencias.p3")))
    //     }
    //     scala.reflect.classTag[Foo].runtimeClass
    //  """
    val code1 =
      q"""
          create doubleMatrix "POTENCIAS" from(matrix("consumo", array("potencias.p1", "potencias.p2", "potencias.p3")))
      """


    //println(code1.children)
    //println(code1.children(0))
    //println(code1.children(0))
    //println(code1.children(0).children(0).children)

    code1.foreach(println)


    val q"create doubleMatrix $x from" = code1

    println(x)

    //val result1 = toolbox.compile(code1)

    //toolbox.eval(toolbox.parse())

    //println(result1)
    println("bye")
  }

}