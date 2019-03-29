package E8

import Setup.sc
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object Exercise4 extends App{

  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(30), Seconds(3))
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  wordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()

  ssc.start()

  ssc.stop(false)
}
