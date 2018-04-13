package es.us.idea.exceptions

final case class PathNotFoundException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
