package E7

import E7.Exercise1.b

object Exercise2 {
  ////////// Exercise 2: approximating membership (Bloom filter)

  // Exact result
  b.filter("hashtag = '#vaccino'").limit(1).count() // returns 1
  b.filter("hashtag = '#vaccino2'").limit(1).count() // returns 0

  // bloomFilter triggers an action; n=1000, p=0.01
  val bf = b.stat.bloomFilter("hashtag", 1000, 0.01)
  bf.mightContain("#vaccino") // returns true
  bf.mightContain("#vaccino2") // probably returns false

  val bf = b.stat.bloomFilter("hashtag", 1000, 10)
  bf.mightContain("#vaccino") // returns true
  bf.mightContain("#vaccino2") // may return true
}
