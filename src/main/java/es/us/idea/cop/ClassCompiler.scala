package es.us.idea.cop

import java.lang.reflect.Method

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe

class ClassCompiler(classStr: String) {
  var instance: Any = _
  var method: Method = _

  def loadClass(classStr: String) = {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val clazz = tb.compile(tb.parse(classStr))().asInstanceOf[Class[_]]
    val ctor = clazz.getDeclaredConstructors()(0)
    instance = ctor.newInstance()
    //val method = instance.getClass.getMethods.filter(m => m.eq("optimusCop"))
    method = instance.getClass.getMethods.filter(m => m.getName.eq("executeCop")).head
  }

  def callMethod(in: Map[String, Any]): Map[String, Any] = {
    method.invoke(instance, in).asInstanceOf[Map[String, Any]]
  }
}
