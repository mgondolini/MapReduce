package E6

import E6.Exercise1.populationDF
import E6.Exercise3.geoDF
import E6.Exercise5.sortedRDD

object Exercise7 {

  // Execution plan of Ex5
  sortedRDD.explain
  //== Physical Plan ==
  //Sort [zip_code#6 DESC], true, 0
  //+- ConvertToUnsafe
  //   +- Exchange rangepartitioning(zip_code#6 DESC,200), None
  //      +- ConvertToSafe
  //         +- TungstenAggregate(key=[zip_code#6], functions=[(sum(totalpopulation#7),mode=Final,isDistinct=false)], output=[zip_code#6,((sum(totalpopulation),mode=Complete,isDistinct=false) / 1000)#11])
  //            +- TungstenExchange hashpartitioning(zip_code#6,200), None
  //               +- TungstenAggregate(key=[zip_code#6], functions=[(sum(totalpopulation#7),mode=Partial,isDistinct=false)], output=[zip_code#6,sum#14])
  //                  +- Project [zip_code#6,totalpopulation#7]
  //                     +- SortMergeJoin [cast(ZIPCODE#3 as double)], [cast(zip_code#6 as double)]
  //                        :- Sort [cast(ZIPCODE#3 as double) ASC], false, 0
  //                        :  +- TungstenExchange hashpartitioning(cast(ZIPCODE#3 as double),200), None
  //                        :     +- Project [ZIPCODE#3]
  //                        :        +- Filter (COUNTY#4 = Los Angeles)
  //                        :           +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,US_GEOGRAPHY,[Lorg.apache.spark.Partition;@5bc637fc,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=US_GEOGRAPHY, driver=oracle.jdbc.driver.OracleDriver})[ZIPCODE#3,COUNTY#4] PushedFilters: [EqualTo(COUNTY,Los Angeles)]
  //                        +- Sort [cast(zip_code#6 as double) ASC], false, 0
  //                           +- TungstenExchange hashpartitioning(cast(zip_code#6 as double),200), None
  //                              +- ConvertToUnsafe
  //                                 +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,ZIPCODE_POPULATION,[Lorg.apache.spark.Partition;@1ddeb95,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=ZIPCODE_POPULATION})[zip_code#6,totalpopulation#7]

  // Execution plan of Ex6


  //Immagine WebUI

  //OPTIONAL

  //Standard Join
  val joinedRDD = geoDF.filter("county = 'Los Angeles'").join(populationDF,geoDF("ZIPCODE") === populationDF("zip_code"))

  //joinedRDD: org.apache.spark.sql.DataFrame = [STATE_FIPS: decimal(38,0), STATE: string, STATE_ABBR: string, ZIPCODE: string, COUNTY: string, CITY: string, ZIP_CODE: decimal(8,0), TOTALPOPULATION: decimal(15,0), MEDIANAGE: decimal(4,0), TOTALMALES: int, TOTALFEMALES: int]

  joinedRDD.explain

  //Result
  //== Physical Plan ==
  //SortMergeJoin [cast(ZIPCODE#3 as double)], [cast(zip_code#6 as double)]
  //:- Sort [cast(ZIPCODE#3 as double) ASC], false, 0
  //:  +- TungstenExchange hashpartitioning(cast(ZIPCODE#3 as double),200), None
  //:     +- ConvertToUnsafe
  //:        +- Filter (COUNTY#4 = Los Angeles)
  //:           +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,US_GEOGRAPHY,[Lorg.apache.spark.Partition;@14a75d82,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=US_GEOGRAPHY, driver=oracle.jdbc.driver.OracleDriver})[STATE_FIPS#0,STATE#1,STATE_ABBR#2,ZIPCODE#3,COUNTY#4,CITY#5] PushedFilters: [EqualTo(COUNTY,Los Angeles)]
  //+- Sort [cast(zip_code#6 as double) ASC], false, 0
  //   +- TungstenExchange hashpartitioning(cast(zip_code#6 as double),200), None
  //      +- ConvertToUnsafe
  //         +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,ZIPCODE_POPULATION,[Lorg.apache.spark.Partition;@47e4dd92,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=ZIPCODE_POPULATION})[ZIP_CODE#6,TOTALPOPULATION#7,MEDIANAGE#8,TOTALMALES#9,TOTALFEMALES#10]


  //--Broadcast--

  val joinedRDD = geoDF.filter("county = 'Los Angeles'").join(broadcast(populationDF),geoDF("ZIPCODE") === populationDF("zipcode"))
  joinedRDD.explain

  //Result
  //== Physical Plan ==
  //BroadcastHashJoin [cast(ZIPCODE#14 as double)], [cast(zip_code#17 as double)], BuildRight
  //:- ConvertToUnsafe
  //:  +- Filter (COUNTY#15 = Los Angeles)
  //:     +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,US_GEOGRAPHY,[Lorg.apache.spark.Partition;@3c0e3dad,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=US_GEOGRAPHY, driver=oracle.jdbc.driver.OracleDriver})[STATE_FIPS#11,STATE#12,STATE_ABBR#13,ZIPCODE#14,COUNTY#15,CITY#16] PushedFilters: [EqualTo(COUNTY,Los Angeles)]
  //+- ConvertToUnsafe
  //   +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,ZIPCODE_POPULATION,[Lorg.apache.spark.Partition;@7ad8178,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=ZIPCODE_POPULATION})[ZIP_CODE#17,TOTALPOPULATION#18,MEDIANAGE#19,TOTALMALES#20,TOTALFEMALES#21]

}
