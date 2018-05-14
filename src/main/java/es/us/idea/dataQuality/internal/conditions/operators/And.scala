package es.us.idea.dataQuality.internal.conditions.operators

import es.us.idea.dataQuality.internal.conditions.Condition

class And(conditions: Seq[Condition]) extends Operator(conditions){
  def operation(results: Seq[Boolean]): Boolean = results.reduce(_&&_)
}
