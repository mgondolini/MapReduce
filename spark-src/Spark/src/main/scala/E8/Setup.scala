package E8

import org.apache.spark.{SparkConf, SparkContext}

object Setup {

  val sc = new SparkContext(new SparkConf().setAppName("Spark Streaming"))
}
