package E6

import Exercise1.{populationDF, sqlContext}

object Exercise4 {

/*
  Starting from ZIPCODE_POPULATION DataFrame add a column that represents the total population in terms of thousands inhabitants
  - Another DF should be created
  - Take just one decimal
 */

  populationDF.registerTempTable("population")

  val newPopDF = sqlContext.sql("select zipcode, totalPopulation, medianAge , round(totalPopulation/1000,1) as totPopulation_k from population")

  //result
  //newPopDF: org.apache.spark.sql.DataFrame = [zipcode: decimal(8,0), totalpopulation: decimal(15,0), medianage: decimal(4,0), totpopulation_k: decimal(26,1)]

  newPopDF.show()

}
