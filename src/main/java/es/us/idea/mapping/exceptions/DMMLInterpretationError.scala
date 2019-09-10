package es.us.idea.mapping.exceptions

final case class DMMLInterpretationError(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
