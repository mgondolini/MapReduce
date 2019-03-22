package E4.exercise1

import E4.Setup.rddWeather

object ex1 {
  def main(args: Array[String]): Unit = {


    ////// Exercise 1

    // We want the average AND the maximum temperature registered for every month
    rddWeather.filter(_.temperature < 999)
      .map(x => (x.month, x.temperature))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k, v._1 / v._2) })
      .collect()

    rddWeather.filter(_.temperature < 999)
      .map(x => (x.month, x.temperature))
      .reduceByKey((x, y) => {
        if (x < y) y else x
      })
      .collect()

    // Optimize the two jobs by avoiding the repetition of the same computations and by defining a good number of partitions

    val rddCached = rddWeather.filter(_.temperature < 999).map(x => (x.month, x.temperature)).cache()
    rddCached.aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2)).map({ case (k, v) => (k, v._1 / v._2) }).collect()
    rddCached.reduceByKey((x, y) => {
      if (x < y) y else x
    }).collect()

    val rddCachedRepartition = rddWeather.repartition(4).filter(_.temperature < 999).map(x => (x.month, x.temperature)).cache()
    rddCachedRepartition.aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2)).map({ case (k, v) => (k, v._1 / v._2) }).collect()
    rddCachedRepartition.reduceByKey((x, y) => {
      if (x < y) y else x
    }).collect()
    rddCachedRepartition.toDebugString

    val rddCachedCoalesce = rddWeather.coalesce(4).filter(_.temperature < 999).map(x => (x.month, x.temperature)).cache()
    rddCachedCoalesce.aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2)).map({ case (k, v) => (k, v._1 / v._2) }).collect()
    rddCachedCoalesce.reduceByKey((x, y) => {
      if (x < y) y else x
    }).collect()
    rddCachedRepartition.toDebugString

    // Hints:
    // - Verify your persisted data in the web UI
    // - Use either repartition() or coalesce() to define the number of partitions
    // - repartition() shuffles all the data
    // - coalesce() minimizes data shuffling by exploiting the existing partitioning
    // - Verify the execution plan of your RDDs with rdd.toDebugString
  }
}
