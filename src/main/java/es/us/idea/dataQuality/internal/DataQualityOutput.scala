package es.us.idea.dataQuality.internal

case class DataQualityOutput(dataQuality: Double, accuracy: Option[Double], completeness: Option[Double], consistency: Option[Double], credibility: Option[Double])
