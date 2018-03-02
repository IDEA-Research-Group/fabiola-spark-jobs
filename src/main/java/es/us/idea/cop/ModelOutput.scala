package es.us.idea.cop

/**
  * This case class is intended to contain the model definition
  * written in Scala code, using the Choco Solver library methods.
  *
  * @param out       : The COP output, defined by the user
  * @param metrics   : Metrics for this COP
  */
case class ModelOutput (out: Seq[Double], metrics: Seq[Double])
