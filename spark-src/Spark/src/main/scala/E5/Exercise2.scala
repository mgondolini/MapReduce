package E5

import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import Setup.sc

object Exercise2 extends App{

  ////////// Exercise 2: checkpointing
  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
    wordCounts.print()
    newSsc.checkpoint("hdfs:/user/mgondolini/streaming/checkpoint")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/mgondolini/streaming/checkpoint", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)

}
