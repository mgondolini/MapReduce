package E6

import E6.Exercise1.{sc, sqlContext}

object Exercise8 {

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}

  val postcodes = sc.textFile("hdfs:///bigdata/dataset/postcodes/postcodes_uk.csv")
  val schemaString = "postcode county_name ward country_name"

  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
  val rowRDD = postcodes.map(_.split(",")).map(e ⇒ Row(e(0), e(1), e(2), e(3)))
  val postCodeDF = sqlContext.createDataFrame(rowRDD, schema)

  // Cached computation
  postCodeDF.registerTempTable("postCode_cached")
  sqlContext.cacheTable("postCode_cached")

  //find the number of postcode for each ward
  val cachedResult = sqlContext.sql("select ward,count(postcode) n_postcodes from postCode_cached group by ward")

  cachedResult.collect()

  // Not cached computation
  postCodeDF.registerTempTable("postCode_not_cached")
}
