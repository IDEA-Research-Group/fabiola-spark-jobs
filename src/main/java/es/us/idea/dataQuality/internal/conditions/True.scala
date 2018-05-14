package es.us.idea.dataQuality.internal.conditions

class True extends Condition {

  override def evaluate(dqin: Option[Map[String, Any]]): Boolean = true

}
