package es.us.idea.dataQuality

import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.Try

object Common {

  def deserializeSchema(json: String): StructType = {
    Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
      case t: StructType => t
      case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
    }
  }

  val schemaStr = "{\"type\":\"struct\",\"fields\":[{\"name\":\"DH\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ICPInstalado\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"consumo\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"anio\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"diasFacturacion\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaFinLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaInicioLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potencias\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"cups\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosAcceso\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"derechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"distribuidora\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaAltaSuministro\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaLimiteDerechosExtension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimaLectura\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoCambioComercial\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fechaUltimoMovimientoContrato\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"impagos\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"importeGarantia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxActa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potMaxBie\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"potenciaContratada\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p4\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p5\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p6\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"precioTarifa\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"p1\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p3\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadEqMedidaTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"propiedadICPTitular\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tarifa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tension\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoFrontera\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"tipoPerfil\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularTipoPersona\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"titularViviendaHabitual\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"totalFacturaActual\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionCodigoPostal\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionPoblacion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ubicacionProvincia\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"


  val dqStr =
    """
      |{
      |  "accuracy": {
      |    "type": "accuracy",
      |    "weight": 0.2,
      |    "businessRules": [
      |      {
      |        "weight": 0.5,
      |        "condition": {
      |          "type": "matches",
      |          "key": "ubicacionProvincia",
      |          "values": [
      |            "Asturias",
      |            "Burgos",
      |            "Cantabria",
      |            "Lugo",
      |            "Palencia"
      |          ]
      |        }
      |      },
      |      {
      |        "weight": 0.5,
      |        "condition": {
      |          "type": "matches",
      |          "key": "tarifa",
      |          "values": [
      |              "2.0DHA",
      |              "2.1DHA",
      |              "3.1A",
      |              "6.2",
      |              "2.1DHS",
      |              "6.1B",
      |              "6.1A",
      |              "2.1A",
      |              "2.0DHS",
      |              "3.0A",
      |              "6.3",
      |              "2.0A",
      |              "6.4"
      |          ]
      |        }
      |      }
      |    ],
      |    "decisionRules": {
      |      "decisionRules": [
      |        {
      |          "condition": {
      |            "type": "between",
      |            "key": "dq",
      |            "lowerBound": 0.8,
      |            "upperBound": 1.0
      |          },
      |          "value": "good"
      |        },
      |        {
      |          "condition": {
      |            "type": "and",
      |            "conditions": [
      |              {
      |                "type": "lt",
      |                "key": "dq",
      |                "values": [0.8]
      |              },
      |              {
      |                "type": "gt",
      |                "key": "dq",
      |                "values": [0.5]
      |              }
      |            ]
      |          },
      |          "value": "acceptable"
      |        }
      |      ],
      |      "default": "bad"
      |    }
      |  },
      |  "consistency": {
      |    "type": "consistency",
      |    "weight": 0.5,
      |    "businessRules": [
      |      {
      |        "weight": 1.0,
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["3.0A"]
      |              },
      |              "then": {
      |                "type": "gt",
      |                "key": "potenciaContratada.p1",
      |                "values": [15]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["3.0A"]
      |              },
      |              "then": {
      |                "type": "gt",
      |                "key": "potenciaContratada.p2",
      |                "values": [15]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["3.0A"]
      |              },
      |              "then": {
      |                "type": "gt",
      |                "key": "potenciaContratada.p3",
      |                "values": [15]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["3.1A"]
      |              },
      |              "then": {
      |                "type": "let",
      |                "key": "potenciaContratada.p1",
      |                "values": [450]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["3.1A"]
      |              },
      |              "then": {
      |                "type": "let",
      |                "key": "potenciaContratada.p2",
      |                "values": [450]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["3.1A"]
      |              },
      |              "then": {
      |                "type": "let",
      |                "key": "potenciaContratada.p3",
      |                "values": [450]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["2.0DHA", "2.0DHS"]
      |              },
      |              "then": {
      |                "type": "lt",
      |                "key": "potenciaContratada.p1",
      |                "values": [10]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["2.0DHA", "2.0DHS"]
      |              },
      |              "then": {
      |                "type": "lt",
      |                "key": "potenciaContratada.p2",
      |                "values": [10]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["2.0DHA", "2.0DHS"]
      |              },
      |              "then": {
      |                "type": "lt",
      |                "key": "potenciaContratada.p3",
      |                "values": [10]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["2.1DHA", "2.1DHS"]
      |              },
      |              "then": {
      |                "type": "between",
      |                "key": "potenciaContratada.p1",
      |                "lowerBound": 10,
      |                "upperBound": 15
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["2.1DHA", "2.1DHS"]
      |              },
      |              "then": {
      |                "type": "between",
      |                "key": "potenciaContratada.p2",
      |                "lowerBound": 10,
      |                "upperBound": 15
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["2.1DHA", "2.1DHS"]
      |              },
      |              "then": {
      |                "type": "between",
      |                "key": "potenciaContratada.p3",
      |                "lowerBound": 10,
      |                "upperBound": 15
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["6.1A", "6.1B", "6.2", "6.3", "6.4"]
      |              },
      |              "then": {
      |                "type": "gt",
      |                "key": "potenciaContratada.p1",
      |                "values": [450]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["6.1A", "6.1B", "6.2", "6.3", "6.4"]
      |              },
      |              "then": {
      |                "type": "gt",
      |                "key": "potenciaContratada.p2",
      |                "values": [450]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            },
      |            {
      |              "type": "if",
      |              "condition": {
      |                "type": "matches",
      |                "key": "tarifa",
      |                "values": ["6.1A", "6.1B", "6.2", "6.3", "6.4"]
      |              },
      |              "then": {
      |                "type": "gt",
      |                "key": "potenciaContratada.p2",
      |                "values": [450]
      |              },
      |              "else": {
      |                "type": "true"
      |              }
      |            }
      |          ]
      |        }
      |      }
      |    ],
      |    "decisionRules": {
      |      "decisionRules": [
      |        {
      |          "condition": {
      |            "type": "matches",
      |            "key": "dq",
      |            "values": [1.0]
      |          },
      |          "value": "good"
      |        }
      |      ],
      |      "default": "bad"
      |    }
      |  },
      |  "completeness": {
      |    "type": "completeness",
      |    "weight": 0.2,
      |    "businessRules": [
      |      {
      |        "weight": 0.25,
      |        "condition": {
      |          "type": "notNull",
      |          "key": "ubicacionProvincia"
      |        }
      |      },
      |      {
      |        "weight": 0.75,
      |        "condition": {
      |          "type": "notNull",
      |          "key": "tarifa"
      |        }
      |      }
      |    ],
      |    "decisionRules": {
      |      "decisionRules": [
      |        {
      |          "condition": {
      |            "type": "between",
      |            "key": "dq",
      |            "lowerBound": 0.75,
      |            "upperBound": 1.0
      |          },
      |          "value": "good"
      |        },
      |        {
      |          "condition": {
      |            "type": "and",
      |            "conditions": [
      |              {
      |                "type": "lt",
      |                "key": "dq",
      |                "values": [0.75]
      |              },
      |              {
      |                "type": "gt",
      |                "key": "dq",
      |                "values": [0.5]
      |              }
      |            ]
      |          },
      |          "value": "acceptable"
      |        }
      |      ],
      |      "default": "bad"
      |    }
      |  },
      |  "credibility": {
      |    "type": "credibility",
      |    "weight": 0.1,
      |    "businessRules": [
      |      {
      |        "weight": 0.75,
      |        "condition": {
      |          "type": "not",
      |          "condition": {
      |            "type": "matches",
      |            "key": "ubicacionProvincia",
      |            "values": ["Asturias"]
      |          }
      |        }
      |      },
      |      {
      |        "weight": 0.25,
      |        "condition": {
      |          "type": "not",
      |          "condition": {
      |            "type": "matches",
      |            "key": "propiedadICPTitular",
      |            "values": [null, "Empresa distribuidora"]
      |          }
      |        }
      |      }
      |    ],
      |    "decisionRules": {
      |      "decisionRules": [
      |        {
      |          "condition": {
      |            "type": "between",
      |            "key": "dq",
      |            "lowerBound": 0.75,
      |            "upperBound": 1.0
      |          },
      |          "value": "good"
      |        },
      |        {
      |          "condition": {
      |            "type": "and",
      |            "conditions": [
      |              {
      |                "type": "lt",
      |                "key": "dq",
      |                "values": [0.75]
      |              },
      |              {
      |                "type": "gt",
      |                "key": "dq",
      |                "values": [0.5]
      |              }
      |            ]
      |          },
      |          "value": "acceptable"
      |        }
      |      ],
      |      "default": "bad"
      |    }
      |  },
      |  "decisionRules": {
      |    "decisionRules": [
      |      {
      |        "condition": {
      |          "type": "matches",
      |          "key": "consistency",
      |          "values": ["bad"]
      |        },
      |        "value": "bad"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["acceptable"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["bad"]
      |            }
      |          ]
      |        },
      |        "value": "bad"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["acceptable"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["acceptable"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "accuracy",
      |              "values": ["bad", "acceptable"]
      |            }
      |          ]
      |        },
      |        "value": "bad"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["acceptable"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["acceptable"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "accuracy",
      |              "values": ["good"]
      |            }
      |          ]
      |        },
      |        "value": "acceptable"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["acceptable"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["good"]
      |            }
      |          ]
      |        },
      |        "value": "acceptable"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["bad"]
      |            }
      |          ]
      |        },
      |        "value": "bad"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["acceptable", "good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "completeness",
      |              "values": ["bad"]
      |            }
      |          ]
      |        },
      |        "value": "bad"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["acceptable", "good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "completeness",
      |              "values": ["acceptable"]
      |            }
      |          ]
      |        },
      |        "value": "acceptable"
      |      },
      |      {
      |        "condition": {
      |          "type": "and",
      |          "conditions": [
      |            {
      |              "type": "matches",
      |              "key": "consistency",
      |              "values": ["good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "credibility",
      |              "values": ["acceptable", "good"]
      |            },
      |            {
      |              "type": "matches",
      |              "key": "completeness",
      |              "values": ["good"]
      |            }
      |          ]
      |        },
      |        "value": "good"
      |      }
      |    ],
      |    "default": "good"
      |  }
      |}
    """.stripMargin

}
