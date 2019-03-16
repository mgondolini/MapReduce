package exercise1a

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    println("Capra")
    val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")

    rddCapra.collect()
    rddCapra.count()

    val rddCapraWords2 = rddCapra.flatMap(x => x.split(" "))
    rddCapraWords2.collect()

    val rddMapCapra = rddCapraWords2.map(x => (x, 1))
    rddMapCapra.take(5)

    val rddReduceCapra = rddMapCapra.reduceByKey((x, y) => x + y)
    rddReduceCapra.collect()

    println("Divina Commedia")
    val rddDc = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

    val rddDcWords2 = rddDc.flatMap( x => x.split(" "))
    rddDcWords2.collect()
    val rddMapDc = rddDcWords2.map(x => (x, 1))
    rddMapDc.take(5)

    val rddReduceDc = rddMapDc.reduceByKey((x, y) => x + y)
    rddReduceDc.collect()
  }

}
