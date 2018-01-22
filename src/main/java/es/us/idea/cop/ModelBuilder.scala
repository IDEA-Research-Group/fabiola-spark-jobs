package es.us.idea.cop

import org.chocosolver.solver.variables.IntVar


class ModelBuilder(name: String, modelDefinition: ModelDefinition
                   /*May also receive the IN and OUT lists
                   * and write the access to each property
                   * using the map*/) {

  def buildClass(): String = {
    "import org.chocosolver.solver._" +
      "class COPReflect {" +
      "def executeCop(in: Map[String, Any]):Map[String, Any] = {" +
      "val model = new Model(" + name + ")" +
      modelDefinition.domainData + "\n" +
      modelDefinition.variables + "\n" +
      modelDefinition.constraints + "\n" +
      modelDefinition.objective + "\n" +
      modelDefinition.solution + "\n" +
      "}" + "\n" +
      "}" + "\n" +
      "scala.reflect.classTag[COPReflect].runtimeClass"
  }
}
