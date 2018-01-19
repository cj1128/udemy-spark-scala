package me.cjting.spark

import org.apache.log4j._
import org.apache.spark._

object WordCount {
   def main(args: Array[String]) {
     Logger.getLogger("org").setLevel(Level.ERROR)

     val sc = new SparkContext("local[*]", "WordCount")

     val rdd = sc
       .textFile("assets/book.txt")
       .flatMap(line => line.split("\\W+"))
       .map(x => x.toLowerCase)

     val result = rdd
       .map(x => (x, 1))
       .reduceByKey((x, y) => x + y)
       .map(x => (x._2, x._1))
       .sortByKey()

     for (item <- result.collect()) {
       val word = item._2
       val count = item._1
       println(f"$word: $count")
     }
   }
}