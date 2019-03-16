package exercise0

import org.apache.spark.{SparkConf, SparkContext}

object WarmUp {
  def main(args: Array[String]): Unit ={

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    println("Capra")
    val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")

    rddCapra.collect()
    rddCapra.count()

    val rddCapraWords1 = rddCapra.map( x => x.split(" "))
    rddCapraWords1.collect()
    val rddCapraWords2 = rddCapra.flatMap( x => x.split(" "))
    rddCapraWords2.collect()

    println("Divina Commedia")
    val rddDc = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

    rddDc.collect()
    rddDc.count()

    val rddDcWords1 = rddDc.flatMap( x => x.split(" "))
    rddDcWords1.collect()
    val rddDcWords2 = rddDc.flatMap( x => x.split(" "))
    rddDcWords2.collect()

  }
}
