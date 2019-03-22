package E3.exercise1d

import org.apache.spark.{SparkConf, SparkContext}

object SparkInvertedIndex {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    println("Capra")
    val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")

    rddCapra.collect()
    rddCapra.count()

    val rddCapraWords2 = rddCapra.flatMap(x => x.split(" "))
    rddCapraWords2.collect()

    val rddMapCapra = rddCapraWords2.zipWithIndex()
    rddMapCapra.collect()

    val rddGroupCapra = rddMapCapra.groupByKey()
    rddGroupCapra.collect()

    println("Divina Commedia")
    val rddDc = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

    val rddDcWords2 = rddDc.flatMap( x => x.split(" "))
    rddDcWords2.collect()

    val rddMapDc = rddDcWords2.zipWithIndex()
    rddMapDc.collect()

    val rddGroupDc = rddMapDc.groupByKey()
    rddGroupDc.collect()

  }
}
