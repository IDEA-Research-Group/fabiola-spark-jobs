package es.us.idea.mapping.internal.array

class BasicArrayDataMap(fromArray: String) extends ArrayDataMapTrait {
  override def getValue(value: Any): Seq[Any] = {
    Seq(0)
  }
}
