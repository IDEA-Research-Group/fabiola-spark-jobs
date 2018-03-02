package es.us.idea.cop

import java.lang.reflect.Method

import es.us.idea.utils.SparkRowUtils
import org.apache.spark.sql.Row

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe

object ClassCompiler {
  var clazz: Class[_] = _

  def loadClass(classStr: String) {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val clazz = tb.compile(tb.parse(classStr))().asInstanceOf[Class[_]]
    this.clazz = clazz
  }

  def callMethod(classStr: String, row: Row, timeout: Long): ModelOutput = {
    val in = SparkRowUtils.fromRowToMap(row)
    if (clazz == null) loadClass(classStr)
    val ctor = clazz.getDeclaredConstructors()(0)
    val instance = ctor.newInstance()
    val method = instance.getClass.getMethods.filter(_.getName.eq("executeCop")).head


    method.invoke(instance, (in, timeout)).asInstanceOf[ModelOutput]
  }
}
