package es.us.idea.listeners

import es.us.idea.utils.{Statuses}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerStageCompleted}

class FabiolaSparkListener extends SparkListener {

  override def onApplicationStart(ev: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(ev)

    var datasetId = SparkListenerShared.datasetId
    var fabiolaDatabase = SparkListenerShared.fabiolaDatabase
    var appId = ev.appId

    if (datasetId.isDefined) {
      fabiolaDatabase.get.updateDatasetStatus(SparkListenerShared.datasetId.get, Statuses.RUNNING)
    }

    println("AAA: Application Start")
    println(ev.appId)
  }

  override def onApplicationEnd(ev: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(ev)

    var datasetId = SparkListenerShared.datasetId
    var fabiolaDatabase = SparkListenerShared.fabiolaDatabase

    if (datasetId.isDefined) {

      if (SparkListenerShared.hasSuccessfullyFinished)
        fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.FINISHED)
      else {
        fabiolaDatabase.get.updateDatasetStatus(datasetId.get, Statuses.ERROR, SparkListenerShared.errorMsg)
        //fabiolaDatabase.get.updateDatasetErrorMsg(datasetId.get, SparkListenerShared.errorMsg)
      }
    }

    println(s"FINISHED ¿Con exito? ${SparkListenerShared.hasSuccessfullyFinished}")
    println(s"ErrorMsg ${SparkListenerShared.errorMsg}")
    println(ev.time)
  }
}
