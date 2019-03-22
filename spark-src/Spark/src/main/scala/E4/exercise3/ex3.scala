package E4.exercise3
import E4.Setup.{rddStation,rddWeather}

object ex3 {
  def main(args: Array[String]): Unit = {

    ////// Exercise 3

    // Join the Weather and Station RDDs
    rddWeather.filter(_.temperature<999)
    // Hints & considerations:
    // - Join syntax: rdd1.join(rdd2)
    // - Both RDDs should be structured as key-value RDDs with the same key: usaf + wban
    // - Consider partitioning and caching to optimize the join
    // - Careful: it is not enough for the two RDDs to have the same number of partitions; they must have the same partitioner!
    // - Verify the execution plan of the join in the web UI
  }
}
