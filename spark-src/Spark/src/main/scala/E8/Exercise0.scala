package E8

import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import Setup.sc

object Exercise0 extends App{

  // Remember to check the IP address!!

  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.flatMap(_.split(" "))
  val count = words.count()
  count.print()
  ssc.start()

  // Copy/paste this final command to terminate the application

  ssc.stop(false)
}
