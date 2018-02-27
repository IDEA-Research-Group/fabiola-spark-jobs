package es.us.idea.cop

import es.us.idea.dao._

class ModelBuilder(modelDefinition: ModelDefinition
                   /*May also receive the IN and OUT lists
                   * and write the access to each property
                   * using the map*/) {

  def buildClass(): String = {
    "import es.us.idea.utils.Default \n" +
    "import es.us.idea.cop.ModelOutput \n" +
    "import org.chocosolver.solver._ \n" +
    "import org.chocosolver.solver.search.limits.TimeCounter \n" +
    "import org.chocosolver.solver.variables.IntVar \n" +
      "class COPReflect { \n" +
      "def executeCop(params: (Map[String, Any], Long)): ModelOutput = { \n" +
      "val in = params._1 \n" +
      "val timeout = params._2 \n" +
      "val model = new Model(\"" + modelDefinition.name + "\") \n" +
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
