package es.us.idea.mapping.internal.basic

class DoubleBasicDataMap(from: String, to: String) extends BasicDataMap(from, to) {
  def get(in: Map[String, Any]): Option[Double] = super.getValue(in).asDouble()
}
