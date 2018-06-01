package es.us.idea.dataQuality

import es.us.idea.cop.ModelOutput
import es.us.idea.dataQuality.internal.DataQualityOutput

case class DQCOPOutout(copOutout: Option[ModelOutput], dqOutput: Option[DataQualityOutput])
