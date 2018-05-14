package es.us.idea.dataQuality.internal.conditions.valConditions

import es.us.idea.dataQuality.Utils
import es.us.idea.dataQuality.internal.conditions.Condition

abstract class ValueCondition(key: Any, values: Seq[Any]) extends Condition {

  def operation(value: Any): Boolean

  override def evaluate(dqin: Option[Map[String, Any]]): Boolean = {
    if(dqin.isDefined) operation(Utils.getValueOfKey(key.toString, dqin.get))
    else operation(key)

  }
}
