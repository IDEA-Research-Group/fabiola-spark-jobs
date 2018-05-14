package es.us.idea.dataQuality.internal.decisionRules

@SerialVersionUID(100L)
trait DecisionRuleTrait extends Serializable{
  def getValue(input: Option[Map[String, Any]]): Option[String]
}
