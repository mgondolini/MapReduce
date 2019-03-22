package E4.exercise5
import E4.Setup.{rddStation,rddWeather,sc}
object ex5 {
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
  }
}
