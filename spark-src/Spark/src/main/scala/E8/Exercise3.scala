package E8

import org.apache.spark.sql.types.IntegerType
import E8.Exercise2.{sqlContext, sc}

object Exercise3 {

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}

  val geoGermany = sc.textFile("hdfs:////bigdata/dataset/postcodes/geo_germany.txt")
  val schemaString = "id land landkreis city zipcode"

  val schema2 = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = geoGermany.map(_.split(",")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))

  val schema = StructType(List(StructField("id", IntegerType),
     StructField("land", StringType),
     StructField("landkreis", StringType),
     StructField("city", StringType),
     StructField("zipcode", IntegerType)
   ))

  val geoGermanyDf = sqlContext.createDataFrame(rowRDD, schema)

  val geoGermanyDf2 = sqlContext.createDataFrame(rowRDD, schema2)

  geoGermanyDf2.show()
}
