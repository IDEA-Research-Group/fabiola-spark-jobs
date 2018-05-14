package es.us.idea.dataQuality.internal.conditions.operators

import es.us.idea.dataQuality.internal.conditions.Condition

abstract class Operator(conditions: Seq[Condition]) extends Condition {

  override def evaluate(dqin: Option[Map[String, Any]]): Boolean = operation(conditions.map(_.evaluate(dqin)))

  def operation(results: Seq[Boolean]): Boolean
}
