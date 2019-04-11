package E6

import E6.Exercise1.sqlContext

object Exercise3 {

  // https://prasanthkothuri.wordpress.com/2017/12/10/connecting-apache-spark-and-sql-databases/

  //TODO verificare e cambiare url

  // Connection url and the table (can also be query instead of table)
  val url = "jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF"
  val table = "US_GEOGRAPHY"

  // Load the table into DataFrame
  val geoDF = sqlContext.read.format("jdbc").options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"dbtable" -> table)).load()
  // Print schema and display sample rows
  geoDF.printSchema()
  geoDF.show()


  val tableZip = "ZIPCODE_POPULATION"
  // Load the table into DataFrame
  val zipcodeDF = sqlContext.read.format("jdbc").options(Map("url" -> url, "dbtable" -> tableZip)).load()
  // Print schema and display sample rows
  zipcodeDF.printSchema()
  zipcodeDF.show()


//  geoDF.createOrReplaceTempView("geoTableTemp")
  geoDF.registerTempTable("geoTableTemp")
  val geoCalifornia = sqlContext.sql("select * from geoTableTemp where STATE = 'California'")
  geoCalifornia.show()
//
//  geoDF.select("land").filter("state = California").show()
//  zipcodeDF.select("land").filter("state = California")

  zipcodeDF.show(100)


  //---SOLUTIONS-----------------------------------------------------------------------------------

//  geoDF.printSchema()
  // Result
  //root
  // |-- STATE_FIPS: decimal(38,0) (nullable = true)
  // |-- STATE: string (nullable = true)
  // |-- STATE_ABBR: string (nullable = true)
  // |-- ZIPCODE: string (nullable = true)
  // |-- COUNTY: string (nullable = true)
  // |-- CITY: string (nullable = true)

//  popDF.printSchema()
  //root
  // |-- ZIPCODE: decimal(8,0) (nullable = true)
  // |-- TOTALPOPULATION: decimal(15,0) (nullable = true)
  // |-- MEDIANAGE: decimal(4,0) (nullable = true)
  // |-- TOTALMALES: integer (nullable = true)
  // |-- TOTALFEMALES: integer (nullable = true)



//
//  geoDF.registerTempTable("geo")
//  val geoCalifornia = sqlContext.sql("select * from geo where STATE = 'California'")
//  geoCalifornia.show()

  //result
  //18/04/14 10:23:23 WARN hdfs.DFSClient: Slow ReadProcessor read fields took 30001ms (threshold=30000ms); ack: seqno: 16 reply: 0 reply: 0 reply: 0 downstreamAckTimeNanos: 1670630, targets: [DatanodeInfoWithStorage[137.204.72.233:50010,DS-30de5ca3-f69f-4d2b-bbac-efbf284650d4,DISK], DatanodeInfoWithStorage[137.204.72.242:50010,DS-11feecaa-91a0-4eea-960e-f318790e565b,DISK], DatanodeInfoWithStorage[137.204.72.236:50010,DS-27736cd3-c9b7-4aa0-9605-b060ccac0a0b,DISK]]
  //+----------+----------+----------+-------+-----------+-----------+
  //|STATE_FIPS|     STATE|STATE_ABBR|ZIPCODE|     COUNTY|       CITY|
  //+----------+----------+----------+-------+-----------+-----------+
  //|         6|California|        CA|  89439|     Sierra|       null|
  //|         6|California|        CA|  90001|Los Angeles|Los angeles|
  //|         6|California|        CA|  90002|Los Angeles|Los angeles|
  //|         6|California|        CA|  90003|Los Angeles|Los angeles|
  //|         6|California|        CA|  90004|Los Angeles|Los angeles|
  //|         6|California|        CA|  90005|Los Angeles|Los angeles|
  //|         6|California|        CA|  90006|Los Angeles|Los angeles|
  //|         6|California|        CA|  90007|Los Angeles|Los angeles|
  //|         6|California|        CA|  90008|Los Angeles|Los angeles|
  //|         6|California|        CA|  90010|Los Angeles|Los angeles|
  //|         6|California|        CA|  90011|Los Angeles|Los angeles|
  //|         6|California|        CA|  90012|Los Angeles|Los angeles|
  //|         6|California|        CA|  90013|Los Angeles|Los angeles|
  //|         6|California|        CA|  90014|Los Angeles|Los angeles|
  //|         6|California|        CA|  90015|Los Angeles|Los angeles|
  //|         6|California|        CA|  90016|Los Angeles|Los angeles|
  //|         6|California|        CA|  90017|Los Angeles|Los angeles|
  //|         6|California|        CA|  90018|Los Angeles|Los angeles|
  //|         6|California|        CA|  90019|Los Angeles|Los angeles|
  //|         6|California|        CA|  90020|Los Angeles|Los angeles|
  //+----------+----------+----------+-------+-----------+-----------+


//  popDF.show(100)
}
