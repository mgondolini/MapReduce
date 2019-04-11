package E6

import E6.Exercise1.{sc, sqlContext}

object Exercise6 {
  import sqlContext.implicits._
  case class Transaction(street: String, city: String, zip: String, state: String, beds: String, baths: String, sq__ft: String, tipo: String, price: String)

  val transaction_df = sc.textFile("real_estate/real_estate_transactions.txt").map(_.split(";")).map(p => Transaction(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))).toDF()

  transaction_df.registerTempTable("transaction")

  val convertedTransactions_df = sqlContext.sql("select city,(price*1.237) price_eur from transaction")

  convertedTransactions_df.registerTempTable("converted")

  val city_avg = sqlContext.sql("select city,avg(price_eur) avg_price from converted group by city")

  city_avg.count()

}
