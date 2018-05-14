package es.us.idea.dataQuality.internal.dimension

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import es.us.idea.dataQuality.internal.businessRules.BusinessRule
import es.us.idea.dataQuality.internal.decisionRules.DecisionRulesEngine

@SerialVersionUID(100L)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[AccuracyDQDimension], name = "accuracy"),
  new Type(value = classOf[CompletenessDQDimension], name = "completeness"),
  new Type(value = classOf[ConsistencyDQDimension], name = "consistency"),
  new Type(value = classOf[CredibilityDQDimension], name = "credibility")
))
abstract class AbstractDQDimension(weight: Double, businessRules: Seq[BusinessRule], decisionRules: Option[DecisionRulesEngine] = None) extends Serializable {

  def getBusinessRules: Seq[BusinessRule] = businessRules

  def getWeight: Double = weight

  // Unweighted
  def calculateDimensionDQ(dqin: Map[String, Any]): Double = businessRules.map(_.getValue(dqin)).sum

  // Weighted
  def calculateWeightedDimensionDQ(dqin: Map[String, Any]): Double = weight * businessRules.map(_.getValue(dqin)).sum

  def getQualitativeDQ(dqin: Map[String, Any]): Option[String] = {
    if(decisionRules.isEmpty) return None
    val quantitative = calculateDimensionDQ(dqin)
    Option(decisionRules.get.getDecision(Map("dq" -> quantitative)))
  }
}
