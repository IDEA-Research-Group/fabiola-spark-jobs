package es.us.idea.readers

import es.us.idea.readers.exception.FailedToReadException
import org.apache.commons.io.IOUtils

import java.io.FileInputStream
import scala.io.Source
import scala.util.Try

class URLReader(url: String) extends Reader{

  override def getString(): Either[FailedToReadException, String] = {
    try {
      val content = Source.fromURL(url)
      Right(content.mkString)
    } catch {
      case t: Throwable => Left(FailedToReadException(s"Filed to read from URL: ${t.getMessage}", t))
    }
  }

}
