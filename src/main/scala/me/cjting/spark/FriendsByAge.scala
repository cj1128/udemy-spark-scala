package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._

// get average number of friends by age
object FriendsByAge {
  def parseLine(line: String) = {
    val items = line.split(",")

    val age = items(2).toInt

    val numFriends = items(3).toInt

    (age, numFriends)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val rdd = sc
      .textFile("assets/fakefriends.csv")
      .map(parseLine)

    val totalByAge = rdd
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val averageByAge = totalByAge
      .mapValues(x => x._1 / x._2)
      .map(x => (x._2, x._1)) // (Count, Age)
      .sortByKey()

    for (pair <- averageByAge.collect()) {
      val age = pair._2
      val count = pair._1
      println(f"$age: $count")
    }
  }
}