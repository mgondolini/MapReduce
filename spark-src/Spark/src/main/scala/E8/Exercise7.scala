package E8

import E8.Exercise4.dfOnlyKeep

import E8.Exercise3.{geoGermanyDf,geoGermanyDf2}
import E8.Exercise2.{sqlContext, potentialDF, ppeDf}

object Exercise7 {

  geoGermanyDf.registerTempTable("geoGermany")
  potentialDF.registerTempTable("potential")
  ppeDf.registerTempTable("ppe")
  dfOnlyKeep.registerTempTable("attributesKeep")

  val dfGloForZip = sqlContext.sql("select landkeris, sum(potential) as potential from geogermany as g join ppe as p ON g.zipcode=p.zipcode group by landkeris")

  val dfLocForSubtyp = sqlContext.sql("select SUBTYPOLOGY, sum(potential) as potential from potential as p join attributesKeep as a ON a.POS_ID=p.POS_ID group by SUBTYPOLOGY")

  dfGloForZip.registerTempTable("gloForZip")
  dfLocForSubtyp.registerTempTable("locForSubtyp")

  val query = """
               select landkeris, glo.potential-loc.potential as diffGloLoc
               from attributesKeep as att
               join geogermany as geo ON geo.zipcode=att.zipcode
               join locForSubtyp as loc ON loc.SUBTYPOLOGY=att.SUBTYPOLOGY
               join gloForZip as glo ON glo.landkeris=geo.landkeris
              """
  val dfNotAllocated = sqlContext.sql(query)
}
