package es.us.idea.dataQuality.internal.conditions

class False extends Condition {

  override def evaluate(dqin: Option[Map[String, Any]]): Boolean = false

  private def getValue: Boolean = false
}
