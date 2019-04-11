package E7

import E7.Exercise1.b

object Exercise3 {

  ////////// Exercise 3: approximating frequency (Count-Min sketch)

  //Exact result
  b.filter("hashtag = '#vaccino'").count()

  // countMinSketch triggers an action; ε=0.01, 1-δ=0.99, seed=10
  val cms = b.stat.countMinSketch("hashtag",0.01,0.99,10)
  cms.estimateCount("#vaccino")

  val cms = b.stat.countMinSketch("hashtag",0.1,0.99,10)
  cms.estimateCount("#vaccino")

  val cms = b.stat.countMinSketch("hashtag",0.01,0.9,10)
  cms.estimateCount("#vaccino")
}
