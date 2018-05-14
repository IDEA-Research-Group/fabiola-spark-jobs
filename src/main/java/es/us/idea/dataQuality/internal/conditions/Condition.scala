package es.us.idea.dataQuality.internal.conditions

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import es.us.idea.dataQuality.internal.conditions.operators._
import es.us.idea.dataQuality.internal.conditions.valConditions._
import es.us.idea.dataQuality.internal.conditions.valConditions.numeric._

@SerialVersionUID(100L)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[And], name = "and"),
  new Type(value = classOf[Not], name = "not"),
  new Type(value = classOf[Or], name = "or"),
  new Type(value = classOf[Between], name = "between"),
  new Type(value = classOf[Matches], name = "matches"),
  new Type(value = classOf[NotNull], name = "notNull"),
  new Type(value = classOf[GreaterEqThan], name = "get"),
  new Type(value = classOf[GreaterThan], name = "gt"),
  new Type(value = classOf[LessEqThan], name = "let"),
  new Type(value = classOf[LessThan], name = "lt"),
  new Type(value = classOf[If], name = "if"),
  new Type(value = classOf[True], name = "true"),
  new Type(value = classOf[False], name = "false")
))
trait Condition extends Serializable{
  def evaluate(dqin: Option[Map[String, Any]]): Boolean
}
