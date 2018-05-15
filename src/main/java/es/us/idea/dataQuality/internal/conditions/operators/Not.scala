package es.us.idea.dataQuality.internal.conditions.operators

import com.fasterxml.jackson.annotation.JsonProperty
import es.us.idea.dataQuality.internal.conditions.Condition

class Not(@JsonProperty("condition") condition: Condition) extends Operator(Seq(condition)) {

  override def operation(results: Seq[Boolean]): Boolean = !results.head
}
