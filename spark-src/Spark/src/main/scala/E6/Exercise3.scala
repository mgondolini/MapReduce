package E6

import E6.Exercise1.{sqlContex}

object Exercise3 {

  // https://prasanthkothuri.wordpress.com/2017/12/10/connecting-apache-spark-and-sql-databases/

  //TODO verificare e cambiare url

  // Connection url and the table (can also be query instead of table)
  val url = "jdbc:oracle:thin:username/password@hostname:port/service_name"
  val geoTable = "geography"
  val zipcodeTable = "zipcode"

  // Load the table into DataFrame
  val geoDF = sqlContex.read.format("jdbc").options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"US_GEOGRAPHY" -> geoTable)).load()
  // Print schema and display sample rows
  geoDF.printSchema()
  geoDF.show()

  // Load the table into DataFrame
  val zipcodeDF = sqlContex.read.format("jdbc").options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"ZIPCODE_POPULATION" -> zipcodeTable)).load()
  // Print schema and display sample rows
  zipcodeDF.printSchema()
  zipcodeDF.show()

  //TODO controllare tabelle e le soluzioni
//  geoDF.createOrReplaceTempView("geoTableTemp")
//  zipcodeDF.createOrReplaceTempView("zipcodeTemp")
  geoDF.select("land").filter("state = California").show()
  zipcodeDF.select("land").filter("state = California")

  zipcodeDF.limit(100)
}
