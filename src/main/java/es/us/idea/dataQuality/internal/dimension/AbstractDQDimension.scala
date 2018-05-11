package es.us.idea.dataQuality.internal.dimension

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import es.us.idea.dataQuality.internal.businessRules.BusinessRule

@SerialVersionUID(100L)
//@JsonTypeInfo(
//  use = JsonTypeInfo.Id.NAME,
//  include = JsonTypeInfo.As.PROPERTY,
//  property = "type"
//)
//@JsonSubTypes(Array(
//  new Type(value = classOf[AccuracyDQDimension], name = "accuracy"),
//  new Type(value = classOf[CompletenessDQDimension], name = "completeness"),
//  new Type(value = classOf[ConsistencyDQDimension], name = "consistency"),
//  new Type(value = classOf[CredibilityDQDimension], name = "credibility")
//))
abstract class AbstractDQDimension(weightedBusinessRules: Seq[(Double, BusinessRule)]) extends Serializable {

  def getWeightedBusinessRules() = weightedBusinessRules

  def calculateDimensionDQ(in: Map[String, Any]): Double = weightedBusinessRules.map { x => x._1 * x._2.getMetric(in) }.sum
}
