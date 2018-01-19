package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._

object RatingsCounter {
 
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    val lines = sc.textFile("assets/ml-100k/u.data")
    
    val ratings = lines.map(x => x.toString().split("\t")(2))
    
    val results = ratings.countByValue()
    
    val sortedResults = results.toSeq.sortBy(_._1)
    
    sortedResults.foreach(println)
  }
}
