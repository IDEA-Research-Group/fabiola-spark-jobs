package es.us.idea.cop

import org.chocosolver.solver._
import org.chocosolver.solver.search.limits.TimeCounter
import org.chocosolver.solver.variables.IntVar

object COPElectricidadGenerico {

  def main(args: Array[String]): Unit = {
    val res = executeCop(Map(), 1000)

    println(res)
  }

  def executeCop(in: Map[String, Any], objectiveUpperBound: Int) = {

    /** ***********************************************************
     * Datos del problema LOS DECIMALES DEBEN ENTRAR COMO ENTEROS, SACAMOS 2 DECIMALES = MULTIPLICAMOS POR 100 ANTES DE ENTRAR EN EL COP!!!!
     * ***********************************************************/

    val PRECIO = Seq(25, 17, 30, 0, 0, 0)

    val TARIFA = "2.1A"

    /*
    val CONSUMOS = Seq(
      Seq(4300, 5200, 4900),
      Seq(4300, 5500, 5100),
      Seq(4300, 5600, 5400),
      Seq(3000, 4900, 5500),
      Seq(3200, 5000, 4200),
      Seq(2100, 1900, 900),
      Seq(1800, 1900, 800),
      Seq(1700, 1900, 900),
      Seq(3700, 3000, 4700),
      Seq(3200, 5500, 4500),
      Seq(4900, 8200, 4700),
      Seq(4700, 7100, 5200),
      Seq(3700, 5400, 4900),
      Seq(2300, 6400, 5000),
      Seq(1500, 4600, 4300),
      Seq(4100, 4500, 4200),
      Seq(3500, 1800, 4100),
      Seq(2000, 2000, 800),
      Seq(1900, 1900, 800),
      Seq(1600, 1600, 4200),
      Seq(3900, 4700, 4500),
      Seq(4100, 5500, 4800),
      Seq(3500, 8100, 540)
    )*/

    val CONSUMOS = Seq(Seq(1800, 2000, 1700), Seq(2100, 1800, 1700), Seq(1800, 1900, 1800), Seq(1900, 2000, 1900), Seq(2100, 2000, 1900), Seq(2100, 2200, 2100), Seq(2300, 2200, 2200), Seq(1800, 2000, 1800), Seq(1600, 1900, 1800), Seq(1600, 1700, 1800), Seq(1600, 1900, 1700), Seq(1600, 2000, 1700), Seq(1500, 1700, 1900), Seq(1600, 2000, 1500), Seq(1800, 1800, 1800), Seq(2000, 1900, 1700), Seq(2000, 2200, 1900), Seq(2200, 2200, 2100), Seq(2200, 2200, 2200), Seq(2200, 2100, 1900), Seq(1600, 1800, 1700), Seq(2000, 1900, 1700), Seq(1800, 2000, 2100), Seq(1600, 1700, 1600))

    /** Instanciacion modelo */
    val model = new Model("ElectricityCOP")
    //val scale = 10

    /** ***********************************************************
     * Variables
     * ***********************************************************/
    val limiteInferiorPotenciaContratada = 1500
    val limiteSuperiorPotenciaContratada = 45000
    val limiteSuperiorTP = 10000000


    val TP = model.intVarArray("gasto mensual", CONSUMOS.length, IntVar.MIN_INT_BOUND, limiteSuperiorTP)
    val TPTotal = model.intVar("Coste total", IntVar.MIN_INT_BOUND, limiteSuperiorTP)

    val potenciaContratada = model.intVarArray("Potencias contratadas", 3, 0, limiteSuperiorTP)

    val potenciaConsumida = model.intVarMatrix("PM", CONSUMOS.length, 3, 0, limiteSuperiorTP)
    val potenciaFactura = model.intVarMatrix("Potencia Factura", CONSUMOS.length, 3, 0, limiteSuperiorTP)
    val terminoPotencia = model.intVarMatrix("Termino Potencia", CONSUMOS.length, 3, 0, limiteSuperiorTP)

    potenciaContratada(0) = model.intVar("Potencia Contratada 1", limiteInferiorPotenciaContratada, limiteSuperiorPotenciaContratada)
    potenciaContratada(1) = model.intVar("Potencia Contratada 2", limiteInferiorPotenciaContratada, limiteSuperiorPotenciaContratada)
    potenciaContratada(2) = model.intVar("Potencia Contratada 3", limiteInferiorPotenciaContratada, limiteSuperiorPotenciaContratada)


    /** ***********************************************************
      * Restricciones https://tarifaluzhora.es/info/maximetro
      * ***********************************************************/

    for (i <- CONSUMOS.indices) {
      for (j <- 0 until 3) {

        val pm = CONSUMOS(i)(j)
        model.arithm(potenciaConsumida(i)(j), "=", pm).post()

        val precio: Int = PRECIO(j)

        model.ifThen(
          model.arithm(model.intScaleView(potenciaConsumida(i)(j), 100), "<=", model.intScaleView(potenciaContratada(j), 85)),
          model.arithm(model.intScaleView(potenciaFactura(i)(j), 100), "=", model.intScaleView(potenciaContratada(j), 85))
        )

        model.ifThen(
          model.and(
            model.arithm(model.intScaleView(potenciaConsumida(i)(j), 100), ">", model.intScaleView(potenciaContratada(j), 85)),
            model.arithm(model.intScaleView(potenciaConsumida(i)(j), 100), "<", model.intScaleView(potenciaContratada(j), 105))
          ),
          model.arithm(model.intScaleView(potenciaFactura(i)(j), 100), "=", model.intScaleView(potenciaConsumida(i)(j), 100))
        )

        val exceso = model.sum(s"Exceso[$i][$j]",
          model.intMinusView(model.intScaleView(potenciaContratada(j), 105)),
          model.intScaleView(potenciaConsumida(i)(j), 100)
        )
        val sumaYDobleExceso = model.sum(s"SumaYDobleExceso[$i][$j]",
          model.intScaleView(potenciaContratada(j), 100),
          exceso
        )

        model.ifThen(
          model.arithm(model.intScaleView(potenciaConsumida(i)(j), 100), ">=", model.intScaleView(potenciaContratada(j), 105)),
          model.arithm(model.intScaleView(potenciaFactura(i)(j), 100), "=", sumaYDobleExceso )
        )

        model.times(potenciaFactura(i)(j), precio, terminoPotencia(i)(j)).post() // x * a = z
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
    val solution = solver.findOptimalSolution(TPTotal, Model.MINIMIZE, new TimeCounter(model, 50 * 1000000000L))
    //val solution = solver.findOptimalSolution(TPTotal, Model.MINIMIZE)

    val metrics = Seq(solver.getTimeCount.toDouble,solver.getReadingTimeCount.toDouble, (solver.getTimeCount + solver.getReadingTimeCount).toDouble, model.getNbVars.toDouble, model.getNbCstrs.toDouble)

    solver.printStatistics()
    //throw solver.getContradictionException

    if(solution != null){
      ModelOutput(
        Seq(
          solution.getIntVal(TPTotal),
          solution.getIntVal(potenciaContratada(0)),
          solution.getIntVal(potenciaContratada(1)),
          solution.getIntVal(potenciaContratada(2))
        ),
        metrics
      )
    } else {
      ModelOutput(
        Seq(
          -1.0/*Default.DefaultDouble.default*/,
          -1.0/*Default.DefaultDouble.default*/,
          -1.0/*Default.DefaultDouble.default*/,
          -1.0/*Default.DefaultDouble.default*/
        ),
        metrics
      )
    }

  }
}
