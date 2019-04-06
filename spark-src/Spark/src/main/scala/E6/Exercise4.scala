package E6

import Exercise1.{populationDF, sqlContext}

object Exercise4 {

/*
  Starting from ZIPCODE_POPULATION DataFrame add a column that represents the total population in terms of thousands inhabitants
  - Another DF should be created
  - Take just one decimal
 */

  //TODO
  val total = populationDF.where("totalPopulation > 10000") //ok
  total.show()
//  val newPopulationDF = populationDF.withColumn("Total Thousands Population", )



}
