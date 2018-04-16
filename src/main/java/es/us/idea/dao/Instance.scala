package es.us.idea.dao

import org.mongodb.scala.bson.ObjectId

case class Instance(
                     _id: ObjectId,
                     modelDefinition: ObjectId,
                     dataset: ObjectId,
                     in: Seq[String],
                     out: Seq[String],
                     ot: Seq[String],
                     metrics: Boolean,
                     timeout: Int,
                     status: String,
                     errorMsg: Option[String],
                     appId: Option[String]
                   )

