package es.us.idea.readers

import es.us.idea.readers.exception.FailedToReadException

abstract class Reader {
  def getString(): Either[FailedToReadException, String]
}
