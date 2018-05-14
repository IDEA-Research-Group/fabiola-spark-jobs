package es.us.idea.dataQuality.internal.decisionRules

@SerialVersionUID(100L)
class DecisionRulesEngine(decisionRules: Seq[DecisionRule], default: String) extends Serializable {

  def getDecision(input: Map[String, Any]): String = {
    val list = decisionRules.map(_.getValue(Option(input))).filter(_.isDefined)
    if(list.isEmpty) return default else if(list.head.isDefined) list.head.get else default
  }

}
