package es.us.idea.utils

import com.mongodb.client.result.UpdateResult
import es.us.idea.dao.{Credentials, Dataset, Instance, ModelDefinition}
import org.bson.BsonDocument
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import org.mongodb.scala.{MongoClient, MongoCollection}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates._

import scala.concurrent.duration._
import scala.concurrent.Await

/**
  * This class represents a connection with the Fabiola MongoDB database.
  *
  * @constructor create a new connection to the Fabiola MongoDB database from the URI of the database and the
  *              database name.
  * @param databaseUri The URI to the MongoDB database.
  * @param databaseName The name of the database used by Fabiola.
  */
class FabiolaDatabase(databaseUri: String, databaseName: String) {
  val database = MongoClient(databaseUri).getDatabase(databaseName)
  val instanceCodecRegistry = fromRegistries(fromProviders(classOf[Instance]), DEFAULT_CODEC_REGISTRY)
  val modelDefinitionCodecRegistry = fromRegistries(fromProviders(classOf[ModelDefinition]), DEFAULT_CODEC_REGISTRY)
  val datasetCodecRegistry = fromRegistries(fromProviders(classOf[Credentials], classOf[Dataset]), DEFAULT_CODEC_REGISTRY)

  /**
    * Gets an instance given its MongoDB ID
    *
    * @param instanceId The MongoDB ID of the instance to find.
    * @return an instance whose _id field is instanceId
    */
  def getInstance(instanceId: String): Instance = {
    val instances: MongoCollection[Instance] = database.withCodecRegistry(instanceCodecRegistry).getCollection("instances")
    val instanceFuture = instances.find(equal("_id", BsonObjectId(instanceId))).first().toFuture()
    Await.result(instanceFuture, 30 seconds)
  }

  /**
    * Gets a model definition given its MongoDB ID
    *
    * @param modelDefinitionId The MongoDB ID of the model defintiion to find.
    * @return a model defintion whose _id field is instanceId
    */
  def getModelDefinition(modelDefinitionId: String): ModelDefinition = {
    val modelDefinitions: MongoCollection[ModelDefinition] = database.withCodecRegistry(modelDefinitionCodecRegistry).getCollection("modelDefinitions")
    val modelDefinitionFuture = modelDefinitions.find(equal("_id", BsonObjectId(modelDefinitionId))).first().toFuture()
    Await.result(modelDefinitionFuture, 30 seconds)
  }

  /**
    * Gets a dataset given its MongoDB ID
    *
    * @param datasetId The MongoDB ID of the dataset to find.
    * @return a dataset document whose _id field is datasetId
    */
  def getDataset(datasetId: String): Dataset = {
    val datasets: MongoCollection[Dataset] = database.withCodecRegistry(datasetCodecRegistry).getCollection("datasets")
    val datasetsFuture = datasets.find(equal("_id", BsonObjectId(datasetId))).first().toFuture()
    Await.result(datasetsFuture, 30 seconds)
  }

  /**
    * Updates the schema field of a Dataset object
    *
    * @param datasetId
    * @param schema
    * @return the updated Dataset object
    */
  def updateDatasetSchema(datasetId: String, schema: String): UpdateResult = {
    val datasets: MongoCollection[Dataset] = database.withCodecRegistry(datasetCodecRegistry).getCollection("datasets")
    val datasetsFuture = datasets.updateOne(equal("_id", BsonObjectId(datasetId)), set("dsSchema", schema)).toFuture
    Await.result(datasetsFuture, 30 seconds)
  }

  /**
    * Updates the status and the error message of a Dataset object. The errorMsg param is optional. If not set, the
    * errorMsg field of the document will be deleted.
    *
    * @param datasetId
    * @param status
    * @param errorMsg
    * @return the updated Dataset object
    */
  def updateDatasetStatus(datasetId: String, status: Statuses.Value, errorMsg: Option[String] = None): Dataset = {
    val datasets: MongoCollection[Dataset] = database.withCodecRegistry(datasetCodecRegistry).getCollection("datasets")

    val set =
      if(errorMsg.isDefined)
        Document("$set" -> Document(Map("status" -> status.toString, "errorMsg" -> errorMsg.get)))
      else
        Document("$set" -> Document(Map("status" -> status.toString)), "$unset" -> Document(Map( "errorMsg" -> 1)))

    val datasetsFuture = datasets.findOneAndUpdate(equal("_id", BsonObjectId(datasetId)), set).toFuture
    Await.result(datasetsFuture, 30 seconds)
  }

  /**
    * Updates the status and the error message of an Instance object. The errorMsg param is optional. If not set, the
    * errorMsg field of the document will be deleted.
    *
    * @param instanceId
    * @param status
    * @param errorMsg
    * @return the updated Instance object
    */
  def updateInstanceStatus(instanceId: String, status: Statuses.Value, errorMsg: Option[String] = None): Instance = {
    val instances: MongoCollection[Instance] = database.withCodecRegistry(instanceCodecRegistry).getCollection("instances")

    val set =
      if(errorMsg.isDefined)
        Document("$set" -> Document(Map("status" -> status.toString, "errorMsg" -> errorMsg.get)))
      else
        Document("$set" -> Document(Map("status" -> status.toString)), "$unset" -> Document(Map( "errorMsg" -> 1)))

    val instancesFuture = instances.findOneAndUpdate(equal("_id", BsonObjectId(instanceId)), set).toFuture
    Await.result(instancesFuture, 30 seconds)
  }

  /**
    * Updates the appId field of an Instance document.
    *
    * @param instanceId
    * @param appId
    * @return the updated Instance object
    */
  def updateInstanceAppId(instanceId: String, appId: String): Instance = {
    val instances: MongoCollection[Instance] = database.withCodecRegistry(instanceCodecRegistry).getCollection("instances")

    val instancesFuture = instances.findOneAndUpdate(equal("_id", BsonObjectId(instanceId)), set("appId", appId)).toFuture
    Await.result(instancesFuture, 30 seconds)
  }

  /**
    * Updates the duration field of an Instance document.
    *
    * @param instanceId
    * @param duration
    * @return the updated Instance object
    */
  def updateInstanceDuration(instanceId: String, duration: Long): Instance = {
    val instances: MongoCollection[Instance] = database.withCodecRegistry(instanceCodecRegistry).getCollection("instances")

    val instancesFuture = instances.findOneAndUpdate(equal("_id", BsonObjectId(instanceId)), set("duration", duration)).toFuture
    Await.result(instancesFuture, 30 seconds)
  }

}
