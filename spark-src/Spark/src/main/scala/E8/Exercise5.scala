package E8
import Setup.sc
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object Exercise5 extends App{

  ////////// Exercise 5: trending hashtags

  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(60), Seconds(10))
  val columns = lines.filter(_.nonEmpty).map(_.split("\\|"))
  val tweet = columns.map(x => x(2))
  val hashtag = tweet.flatMap(_.split(", "))
  val hashtagCount = hashtag.map(x=>(x,1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
  hashtagCount.print()

  ssc.start()

  ssc.stop(false)
}
