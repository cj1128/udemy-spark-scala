package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._

// get average number of friends by age
object FriendsByName {

  def parseLine(line: String) = {
    val items = line.split(",")

    val name = items(1)

    val numFriends = items(3).toInt

    (name, numFriends)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val lines = sc.textFile("assets/fakefriends.csv")

    val rdd = lines.map(parseLine)

    val totalByName = rdd
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val averageByName = totalByName
      .mapValues(x => x._1 / x._2)
      .map(x => (x._2, x._1)) // (Count, Name)
      .sortByKey()

    for (pair <- averageByName.collect()) {
      val count = pair._1
      val name = pair._2
      println(f"$name: $count")
    }
  }
}