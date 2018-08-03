package es.us.idea.mapping.mapper.internal.basic

trait Field {
  def getValue(in: Map[String, Any])
}
