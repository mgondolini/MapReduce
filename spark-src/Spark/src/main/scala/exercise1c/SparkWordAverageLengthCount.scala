package exercise1c

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordAverageLengthCount {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    println("Capra")
    val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")

    rddCapra.collect()
    rddCapra.count()

    val rddCapraWords2 = rddCapra.flatMap(x => x.split(" "))
    rddCapraWords2.collect()

    val rddMapCapra = rddCapraWords2.filter( _.length>0 ).map(x => (x.substring(0,1), x.length))
    rddMapCapra.collect()

    val rddReduceCapra = rddMapCapra.aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1), (a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
    rddReduceCapra.collect()

    val rddFinalCapra = rddReduceCapra.map({case(k,v) => (k, v._1/v._2)})
    rddFinalCapra.collect()

    println("Divina Commedia")
    val rddDc = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

    val rddDcWords2 = rddDc.flatMap( x => x.split(" "))
    rddDcWords2.collect()

    val rddMapDc = rddDcWords2.filter( _.length>0 ).map(x => (x.substring(0,1), x.length))
    rddMapDc.collect()

    val rddReduceDc = rddMapDc.aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1), (a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
    rddReduceDc.collect()

    val rddFinalDc = rddReduceDc.map({case(k,v) => (k, v._1/v._2)})
    rddFinalDc.collect()
  }

}
