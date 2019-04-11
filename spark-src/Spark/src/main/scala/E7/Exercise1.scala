package E7

import org.apache.spark.{SparkConf, SparkContext}

object Exercise1 {
  ////////// Exercise 1: approximating distinct count (HyperLogLog++)

  import org.apache.spark.sql
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
  val a = sc.textFile("hdfs:/bigdata/dataset/tweet").map(_.split("\\|")).filter(_(0)!="LANGUAGE")
  val b = a.filter(_(2)!="").flatMap(_(2).split(" ")).filter(_!="").map(_.replace(",","")).toDF("hashtag").cache()
  b.collect()

  // Exact result
  b.agg(countDistinct("hashtag")).collect()
  // Approximate result (default relative standard deviation = 0.05)
  b.agg(approxCountDistinct("hashtag")).collect()
  b.agg(approxCountDistinct("hashtag",0.1)).collect()
  b.agg(approxCountDistinct("hashtag",0.01)).collect()
}
