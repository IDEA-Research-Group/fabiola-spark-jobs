package es.us.idea.utils

object Statuses extends Enumeration {
  val RUNNING     = Value("RUNNING")
  val WAITING     = Value("WAITING")
  val ERROR       = Value("ERROR")
  val NOT_STARTED = Value("NOT_STARTED")
  val FINISHED    = Value("FINISHED")
}
