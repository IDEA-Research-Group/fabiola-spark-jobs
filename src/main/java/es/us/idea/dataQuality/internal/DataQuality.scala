package es.us.idea.dataQuality.internal

import es.us.idea.dataQuality.internal.decisionRules.DecisionRulesEngine
import es.us.idea.dataQuality.internal.dimension._

@SerialVersionUID(100L)
class DataQuality(
                   accuracy: Option[AccuracyDQDimension] = None,
                   completeness: Option[CompletenessDQDimension] = None,
                   consistency: Option[ConsistencyDQDimension] = None,
                   credibility: Option[CredibilityDQDimension] = None,
                   decisionRules: Option[DecisionRulesEngine] = None
                 ) extends Serializable {

  // It weights the dimensions DQ
  def calculateDQ(dqin: Map[String, Any]): Double = {
    Seq(accuracy, completeness, consistency, credibility).filter(_.isDefined).map(_.get.calculateWeightedDimensionDQ(dqin)).sum
  }

  // Calculates the DataQuality from the individual dimension DQs
  def calculateDQ(in: Map[String, Any], dimension: String): Option[Double] = {
    dimension match {
      case "accuracy" => if (accuracy.isDefined) Option(accuracy.get.calculateDimensionDQ(in)) else None
      case "completeness" => if (completeness.isDefined) Option(completeness.get.calculateDimensionDQ(in)) else None
      case "consistency" => if (consistency.isDefined) Option(consistency.get.calculateDimensionDQ(in)) else None
      case "credibility" => if (credibility.isDefined) Option(credibility.get.calculateDimensionDQ(in)) else None
      case _ => None
    }
  }

  // Return a map where keys are the dimensions and the values are the dimension objects
  private def getDimensionsMap: Map[String, Option[AbstractDQDimension]] = {
    Map("accuracy" -> accuracy, "completeness" -> completeness, "consistency" -> consistency, "credibility" -> credibility)
  }

  // The final DQ value is weighted.
  def getDqout(in: Map[String, Any]): DataQualityOutput = {
    val attributesMap = getDimensionsMap

    val dqoutMap = attributesMap.map { case (k, v) => (k, if (v.isDefined) {
      val unweightedDimensionDQ = v.get.calculateDimensionDQ(in)
      val weightedDimensionDQ = v.get.getWeight * unweightedDimensionDQ
      val quanlitativeDimDQ = v.get.getQualitativeDQ(in)
      Option(DataQualityDimensionOutput(Option(unweightedDimensionDQ), Option(weightedDimensionDQ), quanlitativeDimDQ))
    } else None)
    }

    val dqQualitative = if(decisionRules.isDefined){
      val dimensionsQualitative = dqoutMap
        .filter{case (k, v) => v.isDefined}
        .map{case (k, v) => (k, v.get.qualitative)}
        .filter{case (k, v) => v.isDefined}
        .map{case (k, v) => (k, v.get)}
      Option(decisionRules.get.getDecision(dimensionsQualitative))
    } else None

    val dq = dqoutMap.filter(_._2.isDefined).map(_._2.get.weighted.get).sum

    DataQualityOutput(
      dq,
      dataQualityQualitative = dqQualitative,
      accuracy = dqoutMap("accuracy"),
      completeness = dqoutMap("completeness"),
      consistency = dqoutMap("consistency"),
      credibility = dqoutMap("credibility")
    )
  }
}

// companion object
object DataQuality {
}