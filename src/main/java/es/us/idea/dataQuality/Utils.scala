package es.us.idea.dataQuality

import es.us.idea.dataQuality.internal.businessRules.BusinessRule

import scala.util.Try

object Utils {
  // Child classes constructor helper
  def getDefaultWeightsBusinessRules(businessRules: Seq[BusinessRule]): Seq[(Double, BusinessRule)] =
    businessRules.map((1.0 / businessRules.length, _))
  def toDouble: (Any) => Option[Double] = {case s: String => Try(s.toDouble).toOption case i: Int => Option(i) case l: Long => Option(l) case f: Float => Option(f) case d: Double => Option(d) case _ => None}
}
