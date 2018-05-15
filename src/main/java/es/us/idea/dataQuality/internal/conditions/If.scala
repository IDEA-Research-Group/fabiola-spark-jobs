package es.us.idea.dataQuality.internal.conditions

import com.fasterxml.jackson.annotation.JsonProperty

class If(
          condition: Condition,
          @JsonProperty("then")
          thenCondition: Condition,
          @JsonProperty("else")
          elseCondition: Condition)
  extends Condition {
  override def evaluate(dqin: Option[Map[String, Any]]): Boolean = if (condition.evaluate(dqin)) thenCondition.evaluate(dqin) else elseCondition.evaluate(dqin)
}

