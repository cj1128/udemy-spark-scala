package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._

object MostPopularHero {
  
  def parseName(str: String) : Option[(Int, String)] = {
    val fields = str.split('"')

    if(fields.length > 1) {
      val id = fields(0).trim.toInt
      val name = fields(1).slice(0, fields(1).length-1)
      return Some((id, name))
    } else {
      return None
    }
  }
  
  // (id, occurence)
  def parseOccurence(str: String) : (Int, Int) = {
    val fields = str.split("\\s+")
    (fields(0).toInt, fields.length - 1)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularHero")
    
    val nameMapping = sc
      .textFile("assets/Marvel-names.txt")
      .flatMap(parseName) // (id, name)
      
    val heroes = sc
      .textFile("assets/Marvel-graph.txt")
      .map(parseOccurence)
      .reduceByKey((x, y) => x + y) // (id, total occurences)
      .map(x => (x._2, x._1)) // (total occurences, id)
      .sortByKey(false)
      
    for((hero, i) <- heroes.take(10).zipWithIndex) {
      val name = nameMapping.lookup(hero._2)(0)  
      println(f"$name is the top ${i + 1} popular hero, it has ${hero._1} occurences") 
    }
  }
}