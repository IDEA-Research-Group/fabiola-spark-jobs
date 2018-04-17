package es.us.idea.exceptions.datasource

final case class UnsupportedFormatException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)