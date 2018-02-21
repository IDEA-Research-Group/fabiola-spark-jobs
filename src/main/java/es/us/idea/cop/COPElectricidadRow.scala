package es.us.idea.cop

import es.us.idea.utils.{Default, SparkRowUtils}
import org.apache.spark.sql.Row
import org.chocosolver.solver.Model
import org.chocosolver.solver.search.limits.TimeCounter
import org.chocosolver.solver.variables.IntVar

import scala.util.Try

object COPElectricidadRow {
  def executeCop(row: Row) = {
    //println(in)
    val in = SparkRowUtils.fromRowToMap(row)

    val consumoActual = in.get("consumo").get.asInstanceOf[Seq[Map[String, Any]]]
    val model = new Model("ElectricityCOP")

    /** ***********************************************************
      * Datos del problema
      * ***********************************************************/
    val scale = 10
    val precioTarifa = in.get("precioTarifa").get.asInstanceOf[Map[String, Double]]

    /** ***********************************************************
      * Variables
      * ***********************************************************/
    val TP = model.intVarArray("gasto mensual", consumoActual.length, IntVar.MIN_INT_BOUND, IntVar.MAX_INT_BOUND)
    val TPTotal = model.intVar("Coste total", IntVar.MIN_INT_BOUND, IntVar.MAX_INT_BOUND)

    val potenciaContratada = model.intVarArray("Potencias contratadas", 3, 0, IntVar.MAX_INT_BOUND)

    val potenciaFactura = model.intVarMatrix("Potencia Factura", consumoActual.length, 3, 0, IntVar.MAX_INT_BOUND)
    val terminoPotencia = model.intVarMatrix("Termino Potencia", consumoActual.length, 3, 0, IntVar.MAX_INT_BOUND)

    potenciaContratada(0) = model.intVar("Potencia Contratada 1", 0, IntVar.MAX_INT_BOUND)
    potenciaContratada(1) = model.intVar("Potencia Contratada 2", 0, IntVar.MAX_INT_BOUND)
    potenciaContratada(2) = model.intVar("Potencia Contratada 3", 0, IntVar.MAX_INT_BOUND)

    /** ***********************************************************
      * Restricciones
      * ***********************************************************/
    //val cien = model.intVar("Cien", 100)
    for (i <- 0 until consumoActual.length) {
      for (j <- 0 until 3) {
        // pm is scaled 10 times
        val pm = scale * ( j match {
          case 0 => math.max(
            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p1").get.asInstanceOf[Double].toInt,
            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p4").get.asInstanceOf[Double].toInt
          )
          case 1 => math.max(
            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p2").get.asInstanceOf[Double].toInt,
            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p5").get.asInstanceOf[Double].toInt
          )
          case 2 => math.max(
            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p3").get.asInstanceOf[Double].toInt,
            consumoActual(i).get("potencias").get.asInstanceOf[Map[String, Any]].get("p6").get.asInstanceOf[Double].toInt
          )
        })

        val precio = j match {
          case 0 => precioTarifa.get("p1").get.toInt
          case 1 => precioTarifa.get("p2").get.toInt
          case 2 => precioTarifa.get("p3").get.toInt
        }

        val dias = consumoActual(i).get("diasFacturacion").get.asInstanceOf[Long].toInt

        model.ifThen(
          model.arithm(model.intScaleView(potenciaContratada(j), 85), ">", pm * 100),
          model.scalar(Array(potenciaFactura(i)(j), potenciaContratada(j)), Array(100, -85), "=", 0)
        )
        model.ifThen(model.arithm(model.intScaleView(potenciaContratada(j), 105), "<", pm * 100),
          model.scalar(Array(potenciaFactura(i)(j), potenciaContratada(j)), Array(100, 210), "=", 300*pm)
        )

        model.ifThen(model.and(model.arithm(model.intScaleView(potenciaContratada(j), 85), "<=", pm * 100), model.arithm(model.intScaleView(potenciaContratada(j), 105), ">=", pm * 100)),
          model.scalar(Array(potenciaFactura(i)(j)), Array(100), "=", pm * 100))

        model.times(potenciaFactura(i)(j), precio * dias, terminoPotencia(i)(j)).post
      }
    }

    // Calculo de TPi
    for (i <- 0 until TP.length)
      model.sum(terminoPotencia(i), "=", TP(i)).post()


    /** ***********************************************************
      * Objetivo
      * ***********************************************************/
    model.sum(TP, "=", TPTotal).post()
    model.setObjective(Model.MINIMIZE, TPTotal)

    /** ***********************************************************
      * Solucion
      * ***********************************************************/
    val solver = model.getSolver
    val solution = solver.findOptimalSolution(TPTotal, Model.MINIMIZE, new TimeCounter(model, 5000000000L))

    val metrics = (solver.getTimeCount.toDouble,solver.getReadingTimeCount.toDouble, (solver.getTimeCount + solver.getReadingTimeCount).toDouble, model.getNbVars, model.getNbCstrs)

    if(solution != null){
      (
        ( solution.getIntVal(TPTotal) / (100.0 * scale), // optimal
          solution.getIntVal(potenciaContratada(0)) / scale.toDouble, // p1
          solution.getIntVal(potenciaContratada(1)) / scale.toDouble, //p2
          solution.getIntVal(potenciaContratada(2)) / scale.toDouble //p3
        ),
        metrics
      )
    } else {
      (
        (
          Default.DefaultDouble.default, // optimal
          Default.DefaultDouble.default, // p1
          Default.DefaultDouble.default, //p2
          Default.DefaultDouble.default //p3
        ),
        metrics
      )
    }

  }
}
