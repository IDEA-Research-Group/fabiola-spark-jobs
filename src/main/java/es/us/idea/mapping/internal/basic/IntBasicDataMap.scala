package es.us.idea.mapping.internal.basic

class IntBasicDataMap(from: String, to: String) extends BasicDataMap(from, to) {
  def get(in: Map[String, Any]): Option[Int] = super.getValue(in).asInt()
}
