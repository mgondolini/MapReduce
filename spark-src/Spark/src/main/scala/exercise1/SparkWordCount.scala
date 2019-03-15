package exercise1

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit ={

    //spark2-submit --class exercise1.SparkWordCount Spark.jar

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")
    println("Capra")

    val capraCollect = rddCapra.collect()
    val capraCount = rddCapra.count()

    println(capraCollect)
    println(capraCount)

    val rddCapraWords = rddCapra.flatMap( x => x.split(" "))
    val reduceCapra = rddCapraWords.map(x => x.length).reduce((x,y) => x+y)

    println(reduceCapra)


    val rddDc = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")
    println("Divina Commedia")

    val dcCollect = rddDc.collect()
    val dcCount = rddDc.count()

    println(dcCollect)
    println(dcCount)

    val rddDcWords = rddDc.flatMap( x => x.split(" "))
    val reduceDc = rddDcWords.map(x => x.length).reduce((x,y) => x+y)

    println(reduceDc)
  }
}
