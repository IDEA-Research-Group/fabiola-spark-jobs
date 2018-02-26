package es.us.idea.utils

import es.us.idea.dao.{Instance, ModelDefinition}
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.{MongoClient, MongoCollection}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Filters.equal
import scala.concurrent.duration._

import scala.concurrent.Await

class FabiolaDatabase(databaseUri: String, databaseName: String) {
  val database = MongoClient(databaseUri).getDatabase(databaseName)
  val instanceCodecRegistry = fromRegistries(fromProviders(classOf[Instance]), DEFAULT_CODEC_REGISTRY)
  val modelDefinitionCodecRegistry = fromRegistries(fromProviders(classOf[ModelDefinition]), DEFAULT_CODEC_REGISTRY)

  def getInstance(instanceId: String): Instance = {
    val instances: MongoCollection[Instance] = database.withCodecRegistry(instanceCodecRegistry).getCollection("instances")
    val instanceFuture = instances.find(equal("_id", BsonObjectId(instanceId))).first().toFuture()
    Await.result(instanceFuture, 30 seconds)
  }

  def getModelDefinition(modelDefinitionId: String): ModelDefinition = {
    val modelDefinitions: MongoCollection[ModelDefinition] = database.withCodecRegistry(modelDefinitionCodecRegistry).getCollection("modelDefinitions")
    val modelDefinitionFuture = modelDefinitions.find(equal("_id", BsonObjectId(modelDefinitionId))).first().toFuture()
    Await.result(modelDefinitionFuture, 30 seconds)
  }
}
