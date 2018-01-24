package me.cjting.spark

import org.apache.log4j._
import org.apache.spark.sql._

object SparkSQL {

  case class Person(id:Int, name:String, age:Int, num_friends:Int)

  def parseLine(str: String) : Person = {
    val fields = str.split(",")
    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark
      .sparkContext
      .textFile("assets/fakefriends.csv")
      .map(parseLine)

    // Infer the schema, and register the DataSet as a table
    import spark.implicits._
    val ds = rdd.toDS

    ds.printSchema()

    ds.createOrReplaceTempView("people")

    spark
      .sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
      .collect()
      .foreach(println)

    spark.stop()
  }
}
