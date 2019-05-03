package main.scala.E8

class Exercise3 {

  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  // SQL context
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  val geo = sc.textFile("hdfs:////bigdata/dataset/postcodes/geo_germany.txt")
  val schemaString = "name surname age"

  val schema2 = StructType(schemaString.split(",").map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = geo.map(_.split(",")).map(e ⇒ Row(e(0).toInt(), e(1), e(2), e(3), e(4).toInt()))
  val schema = StructType(List(StructField("id", IntegerType),
     StructField("land", StringType),
     StructField("landkreis", StringType),
     StructField("city", StringType),
     StructField("zipcode", IntegerType)
   ))

  val geodf = sqlContext.createDataFrame(rowRDD, schema)
}
