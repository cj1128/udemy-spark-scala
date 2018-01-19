package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._
import java.lang.Math.max

import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object DegreesOfSeparation {
  val startHeroID = 5306 // SpiderMan
  val endHeroID = 14 // ADAM 3031
  
  val COLORS = Array("WHITE", "GRAY", "BLACK")
  
  var hitCounter:Option[LongAccumulator] = None
   
  // (Connections, Distance, Color)
  type BFSData = (Array[Int], Int, String)
  
  // (ID, BFSData)
  type BFSNode = (Int, (Array[Int], Int, String))
  
  def convertToNode(str: String) : BFSNode = {
    val fields = str.split("\\s+")

    val id = fields(0).toInt

    val connections = fields.slice(1, fields.length).map(_.toInt)

    val color = if (id == startHeroID) "GRAY" else "WHITE"

    (id, (connections, 9999, color))
  }
  
  def bfsMap(node: BFSNode) : Array[BFSNode] = {
    val result = ArrayBuffer[BFSNode]()

    val id = node._1
    val data = node._2
    val connections = data._1
    val distance = data._2
    var color = data._3
    
    if (color == "GRAY") {
      for (conn <- connections) {
        if (conn == endHeroID) { 
          hitCounter.get.add(1)
        }
        
        val newNode:BFSNode = (conn, (Array(), distance + 1, "GRAY"))
        result += newNode
      }
      
      color = "BLACK"
    }
    
    // add original node
    val thisNode:BFSNode = (id, (connections, distance, color))
    result += thisNode
    
    result.toArray
  }
  
  // return the darker color
  def getDarkColor(a: String, b: String) : String = {
    val indexA = COLORS.indexOf(a)
    val indexB = COLORS.indexOf(b)
    COLORS(max(indexA, indexB))
  }
  
  def mergeNode(a: BFSData, b: BFSData) : BFSData = {
    (a._1 ++ b._1, Math.min(a._2, b._2), getDarkColor(a._3, b._3))
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
     
    val sc = new SparkContext("local[*]", "DegreesOfSeparation")
    var rdd = sc.textFile("assets/Marvel-graph.txt").map(convertToNode)
    
    hitCounter = Some(sc.longAccumulator("Hit Counter"))
    
    for(iteration <- 1 to 10) {
      println(f"Iteration $iteration")
      
      rdd = rdd.flatMap(bfsMap)
      
      println(f"Process ${rdd.count()} values")
      
      val hitCount = hitCounter.get.value
      if (hitCount > 0) {
        println(f"Hit the target hero, from $hitCount different directions")
        return
      }
      
      rdd = rdd.reduceByKey(mergeNode)
    }
  }
}