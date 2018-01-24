package me.cjting.spark

import org.apache.log4j._
import org.apache.spark.sql._

object DataFrames {
  case class Person(id:Int, name:String, age:Int, num_friends:Int)

  def parseLine(str: String) : Person = {
    val fields = str.split(",")
    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DataFrames")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people = spark
      .sparkContext
      .textFile("assets/fakefriends.csv")
      .map(parseLine)
      .toDS()
      .cache()

    println("Here is our inferred schema")
    people.printSchema()

    println("Select the name column")
    people.select("name").orderBy("name").show()

    println("Filter out anyone over 21")
    people.filter(people("age") < 21).orderBy("age").show()

    println("Group by age")
    people.groupBy("age").count().orderBy("age").show()

    println("Make everyone 10 years older")
    people.select(people("name"), people("age") + 10 as "age").orderBy("age").show()

    spark.stop()
  }

}
