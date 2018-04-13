package es.us.idea.exceptions

final case class IllegalDatasourceConfigurationException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
