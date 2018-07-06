package es.us.idea.mapping

//import Helpers._
//
//abstract class FromBasicField[T](path: String)
//abstract class FromBasicFields[T](paths: Seq[Field[T]])
//
//abstract class FromBasicTypeArray[T](path: String, paths: Seq[Field[T]])
//
//
//
//abstract class Field[T]() {
//  def getDefault(): T
//  def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): T
//}
//
//class BasicField[T](path: String, default: T, translations: Option[Map[Any, T]], pipeline: Seq[T => T]) extends Field[T]() {
//  def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): T =
//    in.getValueFromPathOrElse(path, default).getAsOrElse[T](default, translations).applyPipeline(pipeline)
//
//  override def getDefault(): T = default
//}
//
//class BasicFields[T](paths: Seq[Field[T]], reduction: Seq[T] => T, translations: Option[Map[Any, T]], pipeline: Seq[T => T]) extends Field[T](){
//  override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): T =
//    paths.map(x => x.getValue(in)).applyReduction(reduction).applyPipeline(pipeline)
//
//  override def getDefault(): T = paths.map(_.getDefault()).applyReduction(reduction)
//}
//
//class BasicTypesArray[T](path: String, paths: Seq[Field[T]], reduction: Seq[T] => T, pipeline: Seq[T => T]) extends Field[T]() {
//  override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): T =
//    new BasicArrayField[T](path, paths).getValue(in).applyReduction(reduction).applyPipeline(pipeline)
//
//  override def getDefault(): T = paths.map(_.getDefault()).applyReduction(reduction)
//}
//
//abstract class ArrayField[T]() {
//  def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[T]
//}
//
//class BasicArrayField[T](path: String, paths: Seq[Field[T]]) extends ArrayField[T]() {
//  override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[T] = {
//    val default = paths.map(_.getDefault())
//    val s = in.getValueFromPathOrElse(path, default).getAsOrElse[Seq[Any]](default) // TODO comprobar si es una secuencia de Map a Any
//    val map = s.zipWithIndex.map(el => (el._2.toString, el._1)).toMap
//    paths.map(_.getValue(map))
//  }
//}
//
//// TODO Matrix field
//abstract class MatrixField[T]() {
//  def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[Seq[T]]
//}
//
//class BasicMatrixField[T](path: String, paths: Seq[Field[T]], pipeline: Seq[T => T]) extends MatrixField[T]() {
//  override def getValue(in: Map[String, Any])(implicit ev: Any => Option[T]): Seq[Seq[T]] = {
//    val default = Seq(Seq())
//    val s = in.getValueFromPath(path)
//    if(s.isDefined) {
//      s.get match {
//        case p: Seq[Seq[Any]] => default
//        case p: Seq[Map[String, Any]] => default
//
//      }
//    } else default // TODO por defecto: calcular
//  }
//}
//
//// Functions for basic type destination
//object Field {
//  def field[T](path: String, default: T): Field[T] = new BasicField[T](path, default, None, Seq())
//  def field[T](path: String, default: T, translations: Map[Any, T]): Field[T] = new BasicField[T](path, default, Option(translations), Seq())
//  def field[T](path: String, default: T, pipeline: Seq[T => T]): Field[T] = new BasicField[T](path, default, None, pipeline)
//  def field[T](path: String, default: T, translations: Map[Any, T], pipeline: Seq[T => T]): Field[T] = new BasicField[T](path, default, Option(translations), pipeline)
//
//  def field[T](paths: Seq[Field[T]], reduction: Seq[T] => T): Field[T] = new BasicFields[T](paths, reduction, None, Seq())
//  def field[T](paths: Seq[Field[T]], reduction: Seq[T] => T, translations: Map[Any, T]): Field[T] = new BasicFields[T](paths, reduction, Option(translations), Seq())
//  def field[T](paths: Seq[Field[T]], reduction: Seq[T] => T, pipeline: Seq[T => T]): Field[T] = new BasicFields[T](paths, reduction, None, pipeline)
//  def field[T](paths: Seq[Field[T]], reduction: Seq[T] => T, translations: Map[Any, T], pipeline: Seq[T => T]): Field[T] = new BasicFields[T](paths, reduction, Option(translations), pipeline)
//
//  def field[T](path: String, paths: Seq[Field[T]], reduction: Seq[T] => T): Field[T] = new BasicTypesArray[T](path, paths, reduction, Seq())
//  def field[T](path: String, paths: Seq[Field[T]], reduction: Seq[T] => T, pipeline: Seq[T => T]): Field[T] = new BasicTypesArray[T](path, paths, reduction, pipeline)
//
//  // TODO para la matriz, seria algo asi:
//  // field[T](path: String, reducedFields: Field[T], reduction)
//
//}
//
//object Functions4 {
//
////  case class To(to: String)
////
////  case class From(from: String)
////
////  case class FromSeq(fromSeq: String)
////
////  case class Translations[T](translations: Map[Any, T])
////
////  case class Pipeline[T](pipeline: Seq[T => T])
////
////  case class Reduction[T](pipeline: Seq[T] => T)
//
//
//  val inTipo = Map(
//    "array" -> Seq(0.0, 1.0, null, 80, null),
//    "ejemplo" -> Seq(Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(0.0, 1.0), Seq(null, 1.0), null),
//    //"ejemplo" -> Seq(),
//    //"ejemplo" -> Seq(Seq()),
//    "ICPInstalado" -> "Icp no instalado", "derechosExtension" -> 32.91, "tension" -> "3X220/380V", "propiedadEqMedidaTitular" -> "Empresa distribuidora",
//    "potenciaContratada" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 32.91, "p2" -> 32.91, "p1" -> 32.91, "p6" -> 0.0), "impagos" -> "NO", "tipoFrontera" -> 4,
//    "tarifa" -> "3.0A", "ubicacionPoblacion" -> "SOMO", "potMaxBie" -> 32.91, "distribuidora" -> "0027 - VIESGO DISTRIBUCION ELECTRICA, S.L.", "fechaAltaSuministro" -> "24/04/1991", "DH" -> "DH3", "totalFacturaActual" -> 4098.68,
//    "propiedadICPTitular" -> "Empresa distribuidora", "importeGarantia" -> 184.11, "ubicacionCodigoPostal" -> 39140, "cups" -> "ES0027700021513001JL0F", "fechaUltimoMovimientoContrato" -> "03/01/2016", "titularTipoPersona" -> "F", "titularViviendaHabitual" -> "N", "precioTarifa" -> Map("p1" -> 11.0, "p2" -> 7.0, "p3" -> 4.0), "fechaLimiteDerechosExtension" -> "31/12/9999", "fechaUltimoCambioComercial" -> "03/01/2016", "tipoPerfil" -> "Pc", "ubicacionProvincia" -> "Cantabria",
//    "consumo" -> Seq(
//      Map("arrayAnidado" -> Seq(1.0, 5.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 1.0, "p2" -> 2.0, "p1" -> 2.0, "p6" -> 0.0), "anio" -> 2014, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014"),
//      Map("arrayAnidado" -> Seq(2.0, 10.0), "potencias" -> Map("p4" -> 0.0, "p5" -> 0.0, "p3" -> 10.0, "p2" -> 20.0, "p1" -> 20.0, "p6" -> 0.0), "anio" -> 2015, "diasFacturacion" -> 6, "fechaInicioLectura" -> "28/05/2014", "fechaFinLectura" -> "03/06/2014")
//    ),
//    "fechaUltimaLectura" -> "02/02/2016", "potMaxActa" -> 32.91)
//
//
//  // To
//  object BasicType {
//
//    // from
//    object Structure {
//      def fromBasicType[T](path: String, default: T, in: Map[String, Any], pipeline: Seq[T => T], translations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T]) : T = {
//        in.getValueFromPathOrElse(path, default).getAsOrElse[T](default, translations).applyPipeline(pipeline)
//      }
//
//      def fromBasicTypes[T](path: Seq[String], default: T, in: Map[String, Any], reduction: Seq[T] => T, pipeline: Seq[T => T], translations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T]) : T = {
//        path.map(x => fromBasicType[T](x, default, in, pipeline, translations)).applyReduction(reduction)
//      }
//
//      //def fromBasicArray[T](path: String, default: Seq[T], defaultVal: T, in: Map[String, Any], reduction: Seq[T] => T, pipeline: Seq[T => T], translations: Option[Map[Any, T]] = None)(implicit ev: Any => Option[T], ev2: Any => Option[Seq[Any]]) : T = {
//      //  in.getValueFromPathOrElse(path, default).getAsOrElse[Seq[Any]](default).zipWithIndex.map{case (v, i) => v.getAsOrElse[T](default.lift(i).getOrElse(defaultVal), translations).applyPipeline(pipeline)}.applyReduction(reduction)
//      //}
//
//    }
//  }
//
//  def scale(scale: Double)(thisVal: Double): Double = thisVal * scale
//
//  def max(values: Seq[Double]): Double = values.max
//
//  def main(args: Array[String]) = {
//    val a = "a"
//
//    import Field._
//
//    // Destino: Tipo básico. Origen: Tipos básicos procedente de estructura de primer nivel
//    val f = field[Double]("potenciaContratada.p1", -999.0, Seq(scale(1000.0) _))
//    val rf = field[Double](Seq(field("potenciaContratada.p1", -9999.0), field("potenciaContratada.p4", -9999.0)), max _)
//
//    println(f.getValue(inTipo))
//    println(rf.getValue(inTipo))
//
//    // Destino: Tipo básico. Origen: Tipos básicos procedente de array de tipos básicos de primer nivel
//    val raf = field[Double]("array", Seq(field("0", -9999.0), field("1", -9999.0), field("2", -9999.0), field("3", -9999.0), field("4", -9999.0)), max _)
//    println(raf.getValue(inTipo))
//
//
////    println(a.getAs[Double](Option(Map("a" -> 1.5))))
////
////    println(BasicType.Structure.fromBasicType[Double]("potenciaContratada.p1", -999.0, inTipo, Seq(scale(10.0))/*, Option(Map(32.91 -> 329.1))*/))
////    println(BasicType.Structure.fromBasicType[Double]("potenciaContratada.p4", -999.0, inTipo, Seq(scale(10.0))/*, Option(Map(32.91 -> 329.1))*/))
////
////    println(BasicType.Structure.fromBasicTypes[Double](Seq("potenciaContratada.p1", "potenciaContratada.p4"), -999.0, inTipo, max, Seq(scale(10.0))/*, Option(Map(32.91 -> 329.1))*/))
//  }
//
//
//}
//
//
//