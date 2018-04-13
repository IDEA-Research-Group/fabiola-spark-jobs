package es.us.idea.listeners

import es.us.idea.utils.FabiolaDatabase

object SparkListenerShared {
  var fabiolaDatabase: Option[FabiolaDatabase] = None
  var datasetId: Option[String] = None
  var errorMsg: Option[String] = None
  var hasSuccessfullyFinished: Boolean = false

  def setFabiolaDatabase(fabiolaDatabase: FabiolaDatabase) = this.fabiolaDatabase = Option.apply(fabiolaDatabase)
  def setDatasetId(datasetId: String) = this.datasetId = Option.apply(datasetId)
  def setErrorMsg(errorMsg: String) = this.errorMsg = Option.apply(errorMsg)
  def setHasSuccessfullyFinished() = hasSuccessfullyFinished = true
}
