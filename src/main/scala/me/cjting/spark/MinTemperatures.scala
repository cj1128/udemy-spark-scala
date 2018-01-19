package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._
import java.lang.Math.min

object MinTemptures {

  def parseLine(line: String) = {
    val items = line.split(",")

    val stateName = items(0)

    val tempType = items(2)

    val temp = items(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f

    (stateName, tempType, temp)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemptures")

    val lines = sc.textFile("assets/1800.csv")

    val rdd = lines.map(parseLine)

    val minTemps = rdd.filter(v => v._2 == "TMIN").map(v => (v._1, v._3))

    val result = minTemps.reduceByKey((x, y) => min(x, y)).collect()

    for (row <- result) {
      println(f"${row._1} min temperature: ${row._2}%.2f F")
    }
  }
}