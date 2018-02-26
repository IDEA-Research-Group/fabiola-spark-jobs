package es.us.idea

import es.us.idea.utils.FabiolaDatabase

object Test {
  def main(args: Array[String]) = {

    val instanceId = "5a9413d2073e827e80be055c"
    val fabiolaMongo = "mongodb://localhost:27017"
    val databaseStr = "test"

    val fabiolaDatabase = new FabiolaDatabase(fabiolaMongo, databaseStr)

    val instance = fabiolaDatabase.getInstance(instanceId)

    val modelDefinition = fabiolaDatabase.getModelDefinition(instance.modelDefinition.toString)

    print(modelDefinition)

  }
}
