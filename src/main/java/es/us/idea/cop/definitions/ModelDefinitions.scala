package es.us.idea.cop.definitions

import es.us.idea.dao._

object ModelDefinitions {

  val hidrocantabricoDef = ModelDefinition(
    "Hidrocantabrico COP"
    ,
    """
    |    val scale = 10
    |    val consumoActual = in.get("consumo").get.asInstanceOf[Seq[Map[String, Any]]]
    |    val precioTarifa = in.get("precioTarifa").get.asInstanceOf[Map[String, Double]]
  """.stripMargin,
  """
    |    val TP = model.intVarArray("gasto mensual", consumoActual.length, IntVar.MIN_INT_BOUND, IntVar.MAX_INT_BOUND)
    |    val TPTotal = model.intVar("Coste total", IntVar.MIN_INT_BOUND, IntVar.MAX_INT_BOUND)
    |
    |    val potenciaContratada = model.intVarArray("Potencias contratadas", 3, 0, IntVar.MAX_INT_BOUND)
    |
    |    val potenciaFactura = model.intVarMatrix("Potencia Factura", consumoActual.length, 3, 0, IntVar.MAX_INT_BOUND)
    |    val terminoPotencia = model.intVarMatrix("Termino Potencia", consumoActual.length, 3, 0, IntVar.MAX_INT_BOUND)
    |
    |    potenciaContratada(0) = model.intVar("Potencia Contratada 1", 0, IntVar.MAX_INT_BOUND)
    |    potenciaContratada(1) = model.intVar("Potencia Contratada 2", 0, IntVar.MAX_INT_BOUND)
    |    potenciaContratada(2) = model.intVar("Potencia Contratada 3", 0, IntVar.MAX_INT_BOUND)
    |
      """.stripMargin,
  """
    |    for (i <- 0 until consumoActual.length) {
    |      for (j <- 0 until 3) {
    |        val pm = scale * ( j match {
    |          case 0 => math.max(
    |            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p1").get.asInstanceOf[Double].toInt,
    |            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p4").get.asInstanceOf[Double].toInt
    |          )
    |          case 1 => math.max(
    |            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p2").get.asInstanceOf[Double].toInt,
    |            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p5").get.asInstanceOf[Double].toInt
    |          )
    |          case 2 => math.max(
    |            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p3").get.asInstanceOf[Double].toInt,
    |            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p6").get.asInstanceOf[Double].toInt
    |          )
    |        })
    |
    |        val precio = j match {
    |          case 0 => precioTarifa.get("p1").get.toInt
    |          case 1 => precioTarifa.get("p2").get.toInt
    |          case 2 => precioTarifa.get("p3").get.toInt
    |        }
    |
    |        val dias = consumoActual(i).get("diasFacturacion").get.asInstanceOf[Long].toInt
    |
    |        model.ifThen(
    |          model.arithm(model.intScaleView(potenciaContratada(j), 85), ">", pm * 100),
    |          model.scalar(Array(potenciaFactura(i)(j), potenciaContratada(j)), Array(100, -85), "=", 0)
    |        )
    |        model.ifThen(model.arithm(model.intScaleView(potenciaContratada(j), 105), "<", pm * 100),
    |          model.scalar(Array(potenciaFactura(i)(j), potenciaContratada(j)), Array(100, 210), "=", 300*pm)
    |        )
    |
    |        model.ifThen(model.and(model.arithm(model.intScaleView(potenciaContratada(j), 85), "<=", pm * 100), model.arithm(model.intScaleView(potenciaContratada(j), 105), ">=", pm * 100)),
    |          model.scalar(Array(potenciaFactura(i)(j)), Array(100), "=", pm * 100))
    |
    |        model.times(potenciaFactura(i)(j), precio * dias, terminoPotencia(i)(j)).post
    |      }
    |    }
    |    for (i <- 0 until TP.length)
    |      model.sum(terminoPotencia(i), "=", TP(i)).post()
    |
      """.stripMargin,
    """
      |    model.sum(TP, "=", TPTotal).post()
      |    model.setObjective(Model.MINIMIZE, TPTotal)
      |
      """.stripMargin,
    """
      |    val solver = model.getSolver
      |    val solution = solver.findOptimalSolution(TPTotal, Model.MINIMIZE, new TimeCounter(model, timeout*1000000L))
      |
      |    val metrics = Seq(solver.getTimeCount.toDouble,solver.getReadingTimeCount.toDouble, (solver.getTimeCount + solver.getReadingTimeCount).toDouble, model.getNbVars.toDouble, model.getNbCstrs.toDouble)
      |
      |    if(solution != null){
      |      ModelOutput(
      |        Seq(
      |          solution.getIntVal(TPTotal) / (100.0 * scale),
      |          solution.getIntVal(potenciaContratada(0)) / scale.toDouble,
      |          solution.getIntVal(potenciaContratada(1)) / scale.toDouble,
      |          solution.getIntVal(potenciaContratada(2)) / scale.toDouble
      |        ),
      |        metrics
      |      )
      |    } else {
      |      ModelOutput(
      |        Seq(
      |          Default.DefaultDouble.default,
      |          Default.DefaultDouble.default,
      |          Default.DefaultDouble.default,
      |          Default.DefaultDouble.default
      |        ),
      |        metrics
      |      )
      |    }
    """.stripMargin
  )

  def main(args: Array[String]) = {
    println(hidrocantabricoDef.domainData.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\""))
    println(hidrocantabricoDef.variables.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\""))
    println(hidrocantabricoDef.constraints.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\""))
    println(hidrocantabricoDef.objective.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\""))
    println(hidrocantabricoDef.solution.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\""))
  }

}