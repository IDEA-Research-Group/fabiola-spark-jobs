package es.us.idea

object QuickTests {

  def main(args: Array[String]) = {
    val x =  417
    val y = 10
    val z = math.ceil(x.toDouble / y).toInt

    println(z)

    val s = Seq(1, 2, 3, 4, 5)

    val  res = s.scan(0)(_ + _)//.drop(1)

    println(res)
  }

}
