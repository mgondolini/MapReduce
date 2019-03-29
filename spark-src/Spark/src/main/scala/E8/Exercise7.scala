package E8

object Exercise7 extends App {
  def updateFunction( newValues: Seq[(Int,Int)], oldValue: Option[(Int,Int,Int,Double)] ): Option[(Int,Int,Int,Double)] = {
    oldValue.getOrElse(0,0,0,0.0) // to initialize oldValue
    newValues.map(_._1).sum // to sum the first elements of newValues
    // ...
//    Some((totTweets, totSentiment, countSentiment, avgSentiment))
  }
}
