package E4

import org.apache.spark.{SparkConf, SparkContext}

object Setup {
  ////// Setup
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)
}
