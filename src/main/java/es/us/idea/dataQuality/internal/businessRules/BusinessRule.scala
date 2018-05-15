package es.us.idea.dataQuality.internal.businessRules

import es.us.idea.dataQuality.internal.conditions.Condition

@SerialVersionUID(100L)
class BusinessRule(weight: Double, condition: Condition) extends Serializable {

  def operation(dqin: Map[String, Any]): Double = if (condition.evaluate(Option(dqin))) 1.0 * weight else 0.0

  def getValue(dqin: Map[String, Any]): Double = operation(dqin)

}
