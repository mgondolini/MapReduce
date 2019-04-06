package E6

object Exercise1 {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  // SQL context
  val sqlContex = new org.apache.spark.sql.SQLContext(sc)

  // DataFrame Movies
  val moviesDF = sqlContex.jsonFile("/bigdata/dataset/movies")
  moviesDF.show()

  // Dataframe Population
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  val population= sc.textFile("/bigdata/dataset/population/zipcode_population_no_header.csv")
  val schemaString = "zipcode averageAge totalPopulation maleFigure femaleFigure"
  val schema = StructType(schemaString.split("").map(fieldName => StructField(fieldName, StringType, true)))
  val rowRDD = population.map(_.split(";")).map(e => Row(e(0), e(1), e(2), e(3), e(4)))
  val populationDF = sqlContex.createDataFrame(rowRDD, schema)
  populationDF.registerTempTable("population")
  populationDF.show()

  // DataFrame RealEstate
  val realEstateDF = sqlContex.read
    .format("csv")
    .option("delimiter", ";")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs:///bigdata/dataset/real_estate/real_estate_transactions.txt")
  realEstateDF.show()

  // DataFrame Userdata
  val userdataDF = sqlContex.read.load("/bigdata/dataset/userdata")
  userdataDF.show()

}
