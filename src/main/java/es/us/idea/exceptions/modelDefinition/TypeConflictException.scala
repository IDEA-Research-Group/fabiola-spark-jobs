package es.us.idea.exceptions.modelDefinition

final case class TypeConflictException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

