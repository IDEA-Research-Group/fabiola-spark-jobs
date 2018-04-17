package es.us.idea.listeners

import es.us.idea.utils.{Statuses}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerStageCompleted}

/**
  * The FabiolaSparkListener class is a modification of the default SparkListener, which is intended to handle the job
  * statuses during its life cycle.
  */
class FabiolaSparkListener extends SparkListener {

  /**
    * This method is called when the Spark application starts. It receives an object which contains the start timestamp
    * in millis. This method updates the status of the current instance or dataset (depending on whether the datasetId
    * or instanceId field of SparkListenerShared has been set), setting it to RUNNING. Finally, it set the start
    * timestamp in the SparkListenerShared object.
    *
    * @param ev
    */
  override def onApplicationStart(ev: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(ev)

    val datasetId = SparkListenerShared.datasetId
    val instanceId = SparkListenerShared.instanceId
    val fabiolaDatabase = SparkListenerShared.fabiolaDatabase
    val appId = ev.appId

    if (instanceId.isDefined) {
      fabiolaDatabase.get.updateInstanceStatus(instanceId.get, Statuses.RUNNING)
      if (appId.isDefined) fabiolaDatabase.get.updateInstanceAppId(instanceId.get, appId.get)
    } else if (datasetId.isDefined) {
      fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.RUNNING)
    }
    SparkListenerShared.setStartTime(ev.time)
  }

  /**
    * This method is called when the Spark application ends. It receives an object which contains the ens timestamp
    * in millis. This method updates the status of the current instance or dataset (depending on whether the datasetId
    * or instanceId field of SparkListenerShared has been set), setting it either to FINISHED or ERROR, depending on
    * if the hasSuccessfullyFinished attribute of the SparkListenerShared has been set to true. If the status is ERROR,
    * it sets the errorMsg specified in the SparkListenerShared object.
    * It also updates the Instance duration in millis, calculated from the start and end timestamp.
    *
    * @param ev
    */
  override def onApplicationEnd(ev: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(ev)

    val datasetId = SparkListenerShared.datasetId
    val instanceId = SparkListenerShared.instanceId
    val fabiolaDatabase = SparkListenerShared.fabiolaDatabase

    SparkListenerShared.setEndTime(ev.time)

    if (instanceId.isDefined) {
      if (SparkListenerShared.hasSuccessfullyFinished)
        fabiolaDatabase.get.updateInstanceStatus(instanceId.get, Statuses.FINISHED)
      else {
        fabiolaDatabase.get.updateInstanceStatus(instanceId.get, Statuses.ERROR, SparkListenerShared.errorMsg)
      }

      if (SparkListenerShared.startTime.isDefined && SparkListenerShared.endTime.isDefined) {
        val duration = SparkListenerShared.endTime.get - SparkListenerShared.startTime.get // duration is given in millis
        fabiolaDatabase.get.updateInstanceDuration(instanceId.get, duration)
      }

    } else if (datasetId.isDefined) {
      if (SparkListenerShared.hasSuccessfullyFinished)
        fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.FINISHED)
      else {
        fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.ERROR, SparkListenerShared.errorMsg)
      }
    }
  }
}
