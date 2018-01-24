package me.cjting.spark

import org.apache.spark._
import org.apache.log4j._
import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

object PopularMovies {
  
  def parseLine(str: String) = {
    val items = str.split("\t")

    val userID = items(0).toInt

    val movieID = items(1).toInt

    val rating = items(2).toInt

    val timestamp = items(3)

    (movieID, 1)
  }

  def getMovieIDNameMapping() : Map[Int, String] = {
    // Handling character encoding
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("assets/ml-100k/u.item").getLines()

    for(line <- lines) {
      val fields = line.split('|')
      if(fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    
    movieNames
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMovies")

    val lines = sc.textFile("assets/ml-100k/u.data")

    val mapping = sc.broadcast(getMovieIDNameMapping)

    val result = lines.map(parseLine)
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1)) // (Count, MovieID)
      .sortByKey(false)
      .map(x => (mapping.value(x._2), x._1)) // (MovieName, Count)
      .take(10)

    for(r <- result) {
      val moveID = r._1
      val count = r._2
      println(f"$moveID: $count")
    }
  }
}