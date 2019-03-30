package E8

import E8.Setup.sc
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercise6 extends App{

  ////////// Exercise 6: counting tweets by city
  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(60), Seconds(10))
  val columns = lines.filter(_.nonEmpty).map(_.split("\\|"))
  val city = columns.map(x => x(4))

  val cityCount = city.map(x=>(x,1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
  cityCount.print()

  ssc.start()

  ssc.stop(false)
}
