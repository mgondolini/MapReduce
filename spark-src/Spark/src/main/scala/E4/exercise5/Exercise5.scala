package E4.exercise5

import E4.Setup.{rddWeather, sc}

object Exercise5 {
  def main(args: Array[String]): Unit = {
    ////// Exercise 5

    // Clean the cache
    sc.getPersistentRDDs.foreach(_._2.unpersist())

    // Use Spark's web UI to verify the space occupied by the following RDDs:
    import org.apache.spark.storage.StorageLevel._
    val memRdd = rddWeather.sample(false,0.1).repartition(8).cache()
    val memSerRdd = memRdd.map(x=>x).persist(MEMORY_ONLY_SER)
    val diskRdd = memRdd.map(x=>x).persist(DISK_ONLY)

    memRdd.collect()
    memSerRdd.collect()
    diskRdd.collect()

    /* @iugin
     * La serializzazione occupa meno spazio in memorizzazione, ma produce un maggior costo CPU
     * e quindi il Tradeoff è costi-cpu
     */
  }
}
