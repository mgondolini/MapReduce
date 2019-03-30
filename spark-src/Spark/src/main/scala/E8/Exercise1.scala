package E8

import Setup.sc
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object Exercise1 extends App{

  ////////// Exercise 1: word count
  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
  wordCounts.print()
  ssc.start()

  ssc.stop(false)

}
