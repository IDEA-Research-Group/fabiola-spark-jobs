package es.us.idea.mapping.internal.array.collaborators

trait ArrayDataMapTrait {
  def getValue(value: Any): Option[Seq[Option[Any]]]
  // def getValue(value: Any): Seq[Any]
}
