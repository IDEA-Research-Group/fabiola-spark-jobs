package es.us.idea.dataQuality.internal

import es.us.idea.dataQuality.internal.dimension._

@SerialVersionUID(100L)
class DataQuality(
                  accuracy: Option[(Double, AccuracyDQDimension)] = None,
                  completeness: Option[(Double, CompletenessDQDimension)] = None,
                  consistency: Option[(Double, ConsistencyDQDimension)] = None,
                  credibility: Option[(Double, CredibilityDQDimension)] = None
                 ) extends Serializable {

  def calculateDQ(in: Map[String, Any]): Double = {
    Seq(accuracy, completeness, consistency, credibility).filter(_.isDefined).map{x => x.get._1 * x.get._2.calculateDimensionDQ(in)}.sum
  }

  private def getAttributesMap: Map[String, Option[(Double, AbstractDQDimension)]] = {
    Map("accuracy" -> accuracy, "completeness" -> completeness, "consistency" -> consistency, "credibility" -> credibility)
  }

  def calculateDQ(in: Map[String, Any], dimension: String): Option[Double] = {
      dimension match {
        case "accuracy" => if(accuracy.isDefined) Option(accuracy.get._2.calculateDimensionDQ(in)) else None
        case "completeness" => if(completeness.isDefined) Option(completeness.get._2.calculateDimensionDQ(in)) else None
        case "consistency" => if(consistency.isDefined) Option(consistency.get._2.calculateDimensionDQ(in)) else None
        case "credibility" => if(credibility.isDefined) Option(credibility.get._2.calculateDimensionDQ(in)) else None
        case _ => None
      }
  }

  def getDqout(in: Map[String, Any]): DataQualityOutput = {
    val attributesMap = getAttributesMap;
    val dqoutMap = attributesMap.map { case (k, v) => (k, if(v.isDefined) Option(v.get._1 * v.get._2.calculateDimensionDQ(in)) else None)}
    val dataQuality = dqoutMap.filter{case (k, v) => v.isDefined}.map(_._2.get).sum

    DataQualityOutput(dataQuality, dqoutMap("accuracy"), dqoutMap("completeness"), dqoutMap("consistency"), dqoutMap("credibility"))
  }
}

// companion object
object DataQuality {
}