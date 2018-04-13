package es.us.idea.exceptions

final case class UnsupportedDatasourceException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)