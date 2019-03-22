package E4.exercise2

import E4.Setup.rddStation

object ex2 {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.HashPartitioner
    val p = new HashPartitioner(8)

    // Consider the following commands to transform the Station RDD:
    val rddS1 = rddStation.partitionBy(p).keyBy(x => x.usaf + x.wban).cache()
    val rddS2 = rddStation.partitionBy(p).cache().keyBy(x => x.usaf + x.wban)
    val rddS3 = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(p).cache()
    val rddS4 = rddStation.keyBy(x => x.usaf + x.wban).cache().partitionBy(p)
    // Which of these options is better? And why?
  }
}
