package main.scala.E8

class Exercise2 {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  // SQL context
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  val url = "jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF"
  val table = "amordenti.CRM_POS_ATTRIBUTES"
  val attributesDF = sqlContext.read.format("jdbc")
    .options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"dbtable" -> table))
    .load()


}
