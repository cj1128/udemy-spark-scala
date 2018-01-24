package me.cjting.spark

import org.apache.log4j._
import org.apache.spark.sql._
import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
import org.apache.spark.sql.functions._

object PopularMoviesDataSets {

  case class Movie(id: Int)

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

    val movieNames = getMovieIDNameMapping()

    val spark = SparkSession
      .builder()
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val movies = spark
      .sparkContext
      .textFile("assets/ml-100k/u.data")
      .map(line => Movie(line.split("\t")(1).toInt))
      .toDS

    val topMovies = movies
      .groupBy("id")
      .count()
      .orderBy(desc("count"))
      .cache()

    topMovies.show()

    println("Here is the top 10 most popular movies")

    for(r <- topMovies.take(10)) {
      val id = r(0).asInstanceOf[Int]
      val name = movieNames(id)
      val count = r(1)
      println(f"$name: $count")
    }
  }
}
