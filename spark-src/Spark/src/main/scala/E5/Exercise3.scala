package E5

import Setup.sc
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object Exercise3 extends App{

  ////////// Exercise 3: stateful operations
  def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
    Some(oldValue.getOrElse(0) + newValues.sum)
  }

  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.242",9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val cumulativeWordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunction)
    cumulativeWordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
    newSsc.checkpoint("hdfs:/user/mgondolini/streaming/checkpoint2")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/mgondolini/streaming/checkpoint2", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)

}
