package E6

import E6.Exercise1.{moviesDF,populationDF,userdataDF,sqlContex}

object Exercise2 {

  // Population to JSON format
  populationDF.write.format("json").save("mgondolini_population")

  // Userdata to Hive Table
  userdataDF.createOrReplaceTempView("mgUserdataTemp")
  sqlContex.sql("create table mgUserdata as select * from mgUserdataTemp")
  // second method
  userdataDF.write.saveAsTable("mgUserdata")

  // Movies to Parquet
  moviesDF.write.format("parquet").save("mgondolini_movies")

}
