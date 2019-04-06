package E5

import Setup.sc
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercise7 extends App {

  ////////// Exercise 7: counting tweets by city and showing average sentiment

  def updateFunction(newValues: Seq[(Int,Int)], oldValue: Option[(Int,Int,Int,Double)] ): Option[(Int,Int,Int,Double)] = {
    val oldValue2 = oldValue.getOrElse(0,0,0,0.0)
    val totTweets = oldValue2._1 + newValues.map(_._1).sum
    val totSentiment = oldValue2._2 + newValues.map(_._2).sum
    val countSentiment = oldValue2._3 + newValues.size
    val avgSentiment = totSentiment.toDouble/countSentiment.toDouble
    Some((totTweets, totSentiment, countSentiment, avgSentiment))
  }

  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val tweets = lines.filter(_.nonEmpty).map(_.split("\\|"))
    val cities = tweets.filter(x => x(7)!="" && x(7)!="0" && x(3)!="").map(x => ( x(7),(1,x(3).toInt) ) )
    val cumulativeCityCounts = cities.updateStateByKey(updateFunction)
    cumulativeCityCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
    newSsc.checkpoint("hdfs:/user/mgondolini/streaming/checkpoint4")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/mgondolini/streaming/checkpoint4", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)
}
