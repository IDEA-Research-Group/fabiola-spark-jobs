package es.us.idea.readers.exception

import es.us.idea.RootException

class FailedToReadException (private val message: String = "",
                             private val cause: Throwable = None.orNull)
  extends RootException(message, cause)

object FailedToReadException {
  def apply(message: String, cause: Throwable): FailedToReadException = new FailedToReadException(message, cause)
}