package E6

object Exercise1 {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  // SQL context
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  // DataFrame Movies
  val moviesDF = sqlContext.jsonFile("/bigdata/dataset/movies")
  moviesDF.show()

  // Dataframe Population
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  val population= sc.textFile("/bigdata/dataset/population/zipcode_population_no_header.csv")
  val schemaString = "zipcode totalPopulation medianAge maleFigure femaleFigure"
  val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
  val rowRDD = population.map(_.split(";")).map(e => Row(e(0), e(1), e(2), e(3), e(4)))
  val populationDF = sqlContext.createDataFrame(rowRDD, schema)
  populationDF.registerTempTable("population")
  populationDF.show()

  // DataFrame RealEstate spark2
  val realEstateDF = sqlContext.read
    .format("csv")
    .option("delimiter", ";")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .load("hdfs:///bigdata/dataset/real_estate/real_estate_transactions.txt")
  realEstateDF.show()

  //DataFrame RealEstate spark approach
  val transaction_RDD = sc.textFile("real_estate/real_estate_transactions.txt")
  val schema_array = transaction_RDD.take(1)
  val schema_string = schema_array(0)
  val schema = StructType(schema_string.split(';').map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = transaction_RDD.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
  val transaction_DF_tmp = sqlContext.createDataFrame(rowRDD, schema)
  //then remove the first row (which was the schema)
  val transaction_DF = transaction_DF.where("street <> 'street'")

  // DataFrame Userdata
  val userdataDF = sqlContext.read.load("/bigdata/dataset/userdata")
  userdataDF.show()


  //----SOLUTIONS-------------------------------------------------------------

  //ADVANCED - Load to DF a text file with an already existing schema
  import sqlContext.implicits._
  case class Transaction(street: String, city: String, zip: String, state: String, beds: String, baths: String, sq__ft: String, tipo: String, price: String)

  val transaction_df = sc.textFile("real_estate/real_estate_transactions.txt").map(_.split(";")).map(p => Transaction(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))).toDF()

  //Load user data from parquet file
  val parquet_df = sqlContext.read.load("userdata/userdata.parquet")

}
