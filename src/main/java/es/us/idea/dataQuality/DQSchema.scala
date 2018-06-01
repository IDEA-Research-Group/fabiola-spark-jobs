package es.us.idea.dataQuality

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mongodb.spark.MongoSpark
import es.us.idea.dataQuality.internal.DataQuality
import es.us.idea.utils.SparkRowUtils
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

object DQSchema {
  //val provinciasEsp = Source.fromFile("/home/alvaro/datasets/provincias_es").getLines.toSeq
  //val provinciasEsp = Seq("Araba", "Álava", "Albacete", "Alicante", "Alacant", "Almería", "Ávila", "Badajoz", "Balears (Illes)", "Barcelona", "Burgos", "Cáceres", "Cádiz", "Castellón", "Castelló", "Ciudad Real", "Córdoba", "Coruña (A)", "Cuenca", "Girona", "Granada", "Guadalajara", "Gipuzkoa", "Huelva", "Huesca", "Jaén", "León", "Lleida", "Rioja (La)", "Lugo", "Madrid", "Málaga", "Murcia", "Navarra", "Ourense", "Asturias", "Palencia", "Palmas (Las)", "Pontevedra", "Salamanca", "Santa Cruz de Tenerife", "Cantabria", "Segovia", "Sevilla", "Soria", "Tarragona", "Teruel", "Toledo", "Valencia", "València", "Valladolid", "Bizkaia", "Zamora", "Zaragoza", "Ceuta", "Melilla")

  //val tarifas = Source.fromFile("/home/alvaro/datasets/tarifas").getLines.toSeq
  //val tarifas = Seq("2.0DHA", "2.1DHA", "3.1A", "6.2", "2.1DHS", "6.1B", "6.1A", "2.1A", "2.0DHS", "3.0A", "6.3", "2.0A", "6.4")

  def main(args: Array[String]) = {


    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val dq = objectMapper.readValue[DataQuality](Common.dqStr)



    var sparkBuilder = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"DataQuality-Simple-Experimental")
      // .config("spark.extraListeners", "es.us.idea.listeners.FabiolaSparkListener")
      //.config("spark.mongodb.output.uri", s"mongodb://10.141.10.121:27017/dataquality.results")

    val spark = sparkBuilder.getOrCreate()

    val dqFunction = (row: Row) => {
      val dqin = SparkRowUtils.fromRowToMap(row)
      dq.getDqout(dqin)
    }

    val executeDQUdf = udf((row: Row) => {
      dqFunction(row)
    })

    val dqin = Seq("ubicacionProvincia", "tarifa", "propiedadICPTitular", "potenciaContratada").map(col(_))
    // TODO OJO: siempre va a devolver las cuatro métricas.
    //val dqout = Seq("dataQuality", "accuracy", "completness", "credibility", "consistency").zipWithIndex.map(x => col("tempdqout").getItem(x._2).as(x._1))

    def deserializeSchema(json: String): StructType = {
      Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
        case t: StructType => t
        case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
      }
    }

    val str = "{\"type\":\"struct\",\"fields\":[{\"name\":\"DH\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ICPInstalado\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"consumo\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"anio\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"diasFacturacion\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaFinLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaInicioLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potencias\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"cups\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosAcceso\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"distribuidora\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaAltaSuministro\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaLimiteDerechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimaLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoCambioComercial\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoMovimientoContrato\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"impagos\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"importeGarantia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxActa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxBie\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potenciaContratada\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"precioTarifa\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadEqMedidaTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadICPTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tarifa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoFrontera\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoPerfil\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularTipoPersona\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularViviendaHabitual\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"totalFacturaActual\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionCodigoPostal\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionPoblacion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionProvincia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"

    val ds = spark.read.json("/home/alvaro/datasets/hidrocantabrico_split.json")
    //val ds = spark.read.json("hdfs://10.141.10.111:8020/user/snape/cbd/hidrocantabrico.json")
    println(ds.schema.json)
  }
}
