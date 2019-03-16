package exercise1b

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordLengthCount {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    println("Capra")
    val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")

    rddCapra.collect()
    rddCapra.count()

    val rddCapraWords2 = rddCapra.flatMap(x => x.split(" "))
    rddCapraWords2.collect()

    val rddMapCapra = rddCapraWords2.map(x => (x.length,1))
    rddMapCapra.collect()

    println("Divina Commedia")
    val rddDc = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

    val rddDcWords2 = rddDc.flatMap( x => x.split(" "))
    rddDcWords2.collect()

    val rddMapDc = rddDcWords2.map(x => (x.length, 1))
    rddMapDc.collect()

    val rddReduceDc = rddMapDc.reduceByKey((x, y) => x + y)
    rddReduceDc.collect()
  }

}
