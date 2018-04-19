package es.us.idea.dao

import org.mongodb.scala.bson.ObjectId

case class Instance
            (
              _id: ObjectId,
              modelDefinition: ObjectId,
              dataset: ObjectId,
              in: Seq[String],
              out: Seq[String],
              ot: Seq[String],
              metrics: Boolean,
              status: String,
              errorMsg: Option[String],
              appId: Option[String],
              systemConfig: Option[SystemConfig]
            )

case class SystemConfig
            (
              driverCores: Option[String],
              driverMemory: Option[String],
              executorCores: Option[String],
              executorMemory: Option[String]
            )