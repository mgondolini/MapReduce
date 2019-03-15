import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit ={
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    System.out.println("")
  }
}
