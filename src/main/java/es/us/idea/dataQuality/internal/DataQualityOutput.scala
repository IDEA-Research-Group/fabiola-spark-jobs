package es.us.idea.dataQuality.internal

case class DataQualityOutput(
                              dataQuality: Double,
                              dataQualityQualitative: Option[String] = None,

                              accuracy: Option[DataQualityDimensionOutput] = None,
                              completeness: Option[DataQualityDimensionOutput] = None,
                              consistency: Option[DataQualityDimensionOutput] = None,
                              credibility: Option[DataQualityDimensionOutput] = None
                            )
