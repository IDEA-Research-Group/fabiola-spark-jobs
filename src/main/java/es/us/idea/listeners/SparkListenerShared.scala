package es.us.idea.listeners

import es.us.idea.utils.FabiolaDatabase

import scala.collection.concurrent.TrieMap

/**
  * This Object is intended to share some variables with the Fabiola Spark Listener.
  */
object SparkListenerShared {
  var fabiolaDatabase: Option[FabiolaDatabase] = None
  var datasetId: Option[String] = None
  var instanceId: Option[String] = None
  var errorMsg: Option[String] = None
  var hasSuccessfullyFinished: Boolean = false
  var startTime: Option[Long] = None
  var endTime: Option[Long] = None
  //var executorCpuTimes: TrieMap[String, Long] = TrieMap()

  def setFabiolaDatabase(fabiolaDatabase: FabiolaDatabase) = this.fabiolaDatabase = Option.apply(fabiolaDatabase)
  def setDatasetId(datasetId: String) = this.datasetId = Option.apply(datasetId)
  def setInstanceId(instanceId: String) = this.instanceId = Option.apply(instanceId)
  def setErrorMsg(errorMsg: String) = this.errorMsg = Option.apply(errorMsg)
  def setHasSuccessfullyFinished() = hasSuccessfullyFinished = true
  def setStartTime(startTime: Long) = this.startTime = Option.apply(startTime)
  def setEndTime(endTime: Long) = this.endTime = Option.apply(endTime)
}
