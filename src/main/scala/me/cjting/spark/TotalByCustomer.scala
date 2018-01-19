package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._

object TotalByCustomer {

   def parseLine(line: String) = {
     val items = line.split(",")

     val customerID = items(0).toInt

     val amount = items(2).toFloat

     (customerID, amount)
   }
   
   def main(args: Array[String]) {
     Logger.getLogger("org").setLevel(Level.ERROR)

     val sc = new SparkContext("local[*]", "WordCount")

     val lines = sc.textFile("assets/customer-orders.csv")

     val rdd = lines.map(parseLine)

     val result = rdd
       .reduceByKey((x, y) => x + y)
       .map(x => (x._2, x._1))
       .sortByKey().collect()

     for (item <- result) {
       val customerID = item._2
       val total = item._1
       println(f"$customerID: $total")
     }
   }
}