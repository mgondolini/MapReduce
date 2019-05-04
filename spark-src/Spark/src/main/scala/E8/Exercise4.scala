package E8

import E8.Exercise2.{sqlContext, attributesDF}

object Exercise4 {
  attributesDF.registerTempTable("attributes")
  val dfOnlyKeep = sqlContext.sql("select * from attributes where flag=='Keep'")
}
