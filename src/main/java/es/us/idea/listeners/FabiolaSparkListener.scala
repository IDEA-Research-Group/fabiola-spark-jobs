package es.us.idea.listeners

import es.us.idea.utils.{Statuses}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerStageCompleted}

class FabiolaSparkListener extends SparkListener {

  override def onApplicationStart(ev: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(ev)

    var datasetId = SparkListenerShared.datasetId
    var instanceId = SparkListenerShared.instanceId
    var fabiolaDatabase = SparkListenerShared.fabiolaDatabase
    var appId = ev.appId

    if (instanceId.isDefined) {
      fabiolaDatabase.get.updateInstanceStatus(instanceId.get, Statuses.RUNNING)
      if (appId.isDefined) fabiolaDatabase.get.updateInstanceAppId(instanceId.get, appId.get)
    } else if (datasetId.isDefined) {
      fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.RUNNING)
    }

    println(ev.appId)
  }

  override def onApplicationEnd(ev: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(ev)

    var datasetId = SparkListenerShared.datasetId
    var instanceId = SparkListenerShared.instanceId
    var fabiolaDatabase = SparkListenerShared.fabiolaDatabase

    if(instanceId.isDefined) {
      if (SparkListenerShared.hasSuccessfullyFinished)
        fabiolaDatabase.get.updateInstanceStatus(instanceId.get, Statuses.FINISHED)
      else {
        fabiolaDatabase.get.updateInstanceStatus(instanceId.get, Statuses.ERROR, SparkListenerShared.errorMsg)
      }
    } else if (datasetId.isDefined) {
      if (SparkListenerShared.hasSuccessfullyFinished)
        fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.FINISHED)
      else {
        fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.ERROR, SparkListenerShared.errorMsg)
      }
    }

    println(s"FINISHED ¿Con exito? ${SparkListenerShared.hasSuccessfullyFinished}")
    println(s"ErrorMsg ${SparkListenerShared.errorMsg}")
    println(ev.time)
  }
}
