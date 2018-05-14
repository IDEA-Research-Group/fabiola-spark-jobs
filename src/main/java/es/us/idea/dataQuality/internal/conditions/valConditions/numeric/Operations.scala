package es.us.idea.dataQuality.internal.conditions.valConditions.numeric

sealed trait Operations

case object gt extends Operations

case object get extends Operations

case object lt extends Operations

case object let extends Operations

case object between extends Operations
