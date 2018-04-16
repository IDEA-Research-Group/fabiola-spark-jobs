package es.us.idea.dao

import org.mongodb.scala.bson.ObjectId

case class Dataset(_id: ObjectId, name: String, hostname: String, port: String, path: String, format: Option[String], dsSchema: Option[String], datasource: String, credentials: Option[Credentials], status: String, errorMsg: Option[String])
case class Credentials(user: String, password: String)