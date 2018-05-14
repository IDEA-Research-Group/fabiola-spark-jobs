package es.us.idea.dataQuality.internal.conditions

class If(condition: Condition, thenCondition: Condition, elseCondition: Condition) extends Condition {
  override def evaluate(dqin: Option[Map[String, Any]]): Boolean = if (condition.evaluate(dqin)) thenCondition.evaluate(dqin) else elseCondition.evaluate(dqin)
}

