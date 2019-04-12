package E6

import E6.Exercise1.populationDF
import E6.Exercise3.geoDF

object Exercise5 {

/*  Take geography and population DF repeat the same transformations applied in the last exercise pushing down queries on DB
  - Print the first 100 rows
  - OPTIONAL : you can run SQL directly on files if want to save space  */

  val joinedRDD = geoDF.filter("county = 'Los Angeles'").join(populationDF,geoDF("ZIPCODE") === populationDF("zipcode"))
  val aggrRDD = joinedRDD.groupBy("zipcode").agg(sum("totalPopulation")/1000)
  val sortedRDD = aggrRDD.orderBy(desc("zipcode"))

  sortedRDD.take(100).foreach(println)

  //Result
  //[Stage 10:=====================================>                (140 + 1) / 200]19/03/17 10:34:40 WARN nio.NioEventLoop: Selector.select() returned prematurely 512 times in a row; rebuilding selector.
  //[93591,7.28500000000]
  //[93563,0.38800000000]
  //[93553,2.13800000000]
  //[93552,38.15800000000]
  //[93551,50.79800000000]
  //[93550,74.92900000000]
  //[93544,1.25900000000]
  //[93543,13.03300000000]
  //[93536,70.91800000000]
  //..
  //..


}
