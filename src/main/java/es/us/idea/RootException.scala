package es.us.idea

class RootException(private val message: String = "",
                         private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
