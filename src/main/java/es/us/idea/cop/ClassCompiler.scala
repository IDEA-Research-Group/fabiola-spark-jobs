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
    //this(classStr, 0)
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val clazz = tb.compile(tb.parse(classStr))().asInstanceOf[Class[_]]
    val ctor = clazz.getDeclaredConstructors()(0)
    this.instance = ctor.newInstance()
    //val method = instance.getClass.getMethods.filter(m => m.eq("optimusCop"))
    this.method = instance.getClass.getMethods.filter(m => m.getName.eq("executeCop")).head

  }

  def callMethod(row: Row, timeout: Long): ModelOutput = {
    val in = SparkRowUtils.fromRowToMap(row)
    method.invoke(instance, (in, timeout)).asInstanceOf[ModelOutput]
  }
}
