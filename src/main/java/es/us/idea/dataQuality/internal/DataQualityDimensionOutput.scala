package es.us.idea.dataQuality.internal

case class DataQualityDimensionOutput(
                              unweighted: Option[Double] = None,
                              weighted: Option[Double] = None,
                              qualitative: Option[String] = None
                            )
