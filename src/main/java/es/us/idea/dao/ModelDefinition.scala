package es.us.idea.dao

import org.mongodb.scala.bson.BsonObjectId


/**
  * This case class is intended to contain the model definition
  * written in Scala code, using the Choco Solver library methods.
  */
//case class ModelDefinition(name: String, domainData: String, variables: String, constraints: String, objective: String, solution: String, _id: Option[BsonObjectId] = None)
case class ModelDefinition(name: String, definition: String /*, _id: Option[BsonObjectId] = None*/)

