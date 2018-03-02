package es.us.idea.cop

import java.lang.reflect.Method

import es.us.idea.utils.SparkRowUtils
import org.apache.spark.sql.Row

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe

object ClassCompiler {
  var instance: Any = _
  var method: Method = _

  def loadClass(classStr: String) {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val clazz = tb.compile(tb.parse(classStr))().asInstanceOf[Class[_]]
    val ctor = clazz.getDeclaredConstructors()(0)
    this.instance = ctor.newInstance()
    this.method = instance.getClass.getMethods.filter(_.getName.eq("executeCop")).head
  }

  def callMethod(classStr: String, row: Row, timeout: Long): ModelOutput = {
    val in = SparkRowUtils.fromRowToMap(row)
    if (method == null) loadClass(classStr)
    method.invoke(instance, (in, timeout)).asInstanceOf[ModelOutput]
  }
}
