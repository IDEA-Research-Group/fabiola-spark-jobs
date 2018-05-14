package es.us.idea.dataQuality.internal.conditions.operators

import es.us.idea.dataQuality.internal.conditions.Condition

class Not(condition: Condition) extends Operator(Seq(condition)) {

  override def operation(results: Seq[Boolean]): Boolean = !results.head
}
