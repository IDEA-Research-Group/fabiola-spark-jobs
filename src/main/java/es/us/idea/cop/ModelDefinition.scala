package es.us.idea.cop

/** *****************************************************************
  * This case class is intended to contain the model definition
  * written in Scala code, using the Choco Solver library methods.
  *
  * @param domainData  : Data shared by all the Constraint Optimization
  *                    Problem instances
  * @param variables   : Declaration of the variables common to all the
  *                    Constraint Optimization problem instances
  * @param constraints : Declaration of the constraints
  * @param objective   : Definition of the objective function
  * @param solution    : Definition of the sentence that declares the
  *                    strategy to follow in order to solve the model
  * *****************************************************************/
case class ModelDefinition(domainData: String, variables: String, constraints: String, objective: String, solution: String)
