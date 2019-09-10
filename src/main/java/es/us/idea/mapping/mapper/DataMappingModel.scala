package es.us.idea.mapping.mapper

import es.us.idea.mapping.exceptions.DMMLInterpretationError
import es.us.idea.mapping.mapper.dsl.DSL
import es.us.idea.mapping.mapper.dsl.DSL.FieldGetter

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter._
import javax.script._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes

import scala.util.Try

// DMML = Data Model Language
class DataMappingModel(dmml: Seq[String]) {

  loadFieldGetters // called when the object is constructed

  var fieldGetters: Seq[FieldGetter] = _

  private def loadFieldGetters = {
    val engine = new ScriptEngineManager().getEngineByName("scala")
    val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
    settings.embeddedDefaults[FieldGetter]
    settings.usejavacp.value = true

    engine.eval(""" import es.us.idea.mapping.mapper.dsl.DSL._ """)
    val fieldGetterOpt = Try(dmml.map(x => engine.eval(x).asInstanceOf[FieldGetter])).toOption
    if(fieldGetterOpt.isEmpty) throw new DMMLInterpretationError("Error interpreting the DMML commands.")
    fieldGetters = fieldGetterOpt.get
  }

  // TODO mejorar la gestión de excepciones
  def getResultSchema = {
    DataTypes.createStructType(
      fieldGetters.map(x => {
        DataTypes.createStructField(x.getName(),
          x.getType() match {
            case i: DSL.Types.int.type => DataTypes.IntegerType
            case d: DSL.Types.double.type => DataTypes.DoubleType
            case ia: DSL.Types.intArray.type => DataTypes.createArrayType(DataTypes.IntegerType)
            case da: DSL.Types.doubleArray.type => DataTypes.createArrayType(DataTypes.DoubleType)
            case im: DSL.Types.intMatrix.type => DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType))
            case dm: DSL.Types.doubleMatrix.type => DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType))
            case _ => DataTypes.NullType
          }, true)
      }).toArray
    )
  }

  def getResultMap(in: Map[String, Any]) = {
    fieldGetters.map(x => (x.getName, x.getValue(in))).toMap
  }

  def getResultRow(in: Map[String, Any]) = {
    Row.apply(getResultMap(in).values.toSeq: _*)
  }

}
