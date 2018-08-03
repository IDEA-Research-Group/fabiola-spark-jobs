package es.us.idea.mapping.combiner

object DMSelector {

  implicit class CreateSelectedFieldContainer(val str: String) {
    def from(from: String) = SelectedFieldContainer(str, from)
  }

  case class SelectedFieldContainer(field: String, dataSource: String, as: Option[String] = None) {
    def as(as: String) = SelectedFieldContainer(field, dataSource, Option(as))
    def keyName = as.getOrElse(field)
  }

  case class select(args: SelectedFieldContainer *) {
    // the apply function must be applied to each map to convert
    def apply(mapped: Map[String, Map[String, Any]]): Map[String, Any] = {
      args.map(x => (x.keyName, mapped.get(x.dataSource).get.get(x.field))).toMap
    }
  }



  //def applySelect(s: select) = {
  //
  //}

  def main(args: Array[String]) = {

    select("A" from "DS1" as "A2") //.apply(Map())

    println("aa")

  }



}
