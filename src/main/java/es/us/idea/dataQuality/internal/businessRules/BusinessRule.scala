package es.us.idea.dataQuality.internal.businessRules

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import es.us.idea.dataQuality.internal.businessRules.basic.numeric.{FieldGreaterEqThanBR, FieldGreaterThanBR, FieldLessEqThanBR, FieldLessThanBR}
import es.us.idea.dataQuality.internal.businessRules.basic.{FieldBetweenBR, FieldMatchesBR, FieldNotNullBR}
import es.us.idea.dataQuality.internal.businessRules.operations.{AndBRO, IfBRO, NotBRO}


@SerialVersionUID(100L)
//@JsonTypeInfo(
//  use = JsonTypeInfo.Id.NAME,
//  include = JsonTypeInfo.As.PROPERTY,
//  property = "type"
//)
//@JsonSubTypes(Array(
//  new Type(value = classOf[FieldMatchesBR], name = "matches"),
//  new Type(value = classOf[FieldNotNullBR], name = "notNull"),
//  new Type(value = classOf[FieldBetweenBR], name = "between"),
//  new Type(value = classOf[FieldGreaterThanBR], name = "gt"),
//  new Type(value = classOf[FieldGreaterEqThanBR], name = "get"),
//  new Type(value = classOf[FieldLessThanBR], name = "lt"),
//  new Type(value = classOf[FieldLessEqThanBR], name = "let"),
//  new Type(value = classOf[NotBRO], name = "not"),
//  new Type(value = classOf[AndBRO], name = "and"),
//  new Type(value = classOf[IfBRO], name = "if")
//))
trait BusinessRule extends Serializable {
  def getMetric(in: Map[String, Any]): Int

  def evaluate(in: Map[String, Any]): Boolean = if (getMetric(in) == 1) true else false
}
