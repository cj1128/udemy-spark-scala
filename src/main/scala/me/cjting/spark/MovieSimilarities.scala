package me.cjting.spark;

import java.nio.charset.CodingErrorAction
import org.apache.spark._
import org.apache.log4j._
import scala.io.{Codec, Source}
import Math.sqrt

object MovieSimilarities {

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

  // (UserID, (MovieID, Rating))
  def parseLine(str: String) : (Int, (Int, Double)) = {
    val fields = str.split('\t')
    (fields(0).toInt, (fields(1).toInt, fields(2).toDouble))
  }

  type MovieRatingPair = ((Int, Double), (Int, Double))
  type UserRatingPair = (Int, MovieRatingPair)
  def duplicateFilter(data: UserRatingPair) : Boolean = {
    val pair = data._2
    val r1 = pair._1
    val r2 = pair._2
    return r1._1 < r2._1
  }

  // convert (Rating1, Rating2) -> (Similarity, Occurence)
  // https://en.wikipedia.org/wiki/Cosine_similarity
  def calcCosineSimilarity(pairs: Iterable[(Double, Double)]) : (Double, Int) = {
    var sum_xx:Double = 0
    var sum_yy:Double = 0
    var sum_xy:Double = 0

    for (pair <- pairs) {
      val x = pair._1
      val y = pair._2
      sum_xx += x * x
      sum_yy += y * y
      sum_xy += x * y
    }

    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    val similarity = if (denominator == 0) 0 else sum_xy / denominator
    (similarity, pairs.size)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MovieSimilarities")

    println("Load movie names...")
    val nameMapping = getMovieIDNameMapping()

    val ratings = sc
      .textFile("assets/ml-100k/u.data")
      .map(parseLine) // (UserID, (MovieID, Rating))

    val movieSimilarities = ratings
      .join(ratings) // (UserID, ((MovieID1, Rating1), (MovieID2, Rating2))
      .filter(duplicateFilter) // as above, but remove duplications
      .map(item => {
        val pair = item._2
        val id1 = pair._1._1
        val rating1 = pair._1._2
        val id2 = pair._2._1
        val rating2 = pair._2._2
        ((id1, id2), (rating1, rating2))
      }) // ((MovieID1, MovieID2), (Rating1, Rating2))
      .groupByKey() // ((MovieID1, MovieID2), [(Rating1, Rating2)...])
      .mapValues(calcCosineSimilarity) // ((MovieID1, Movie2), (Similarity, Occurence))
      .cache()

    if (args.length > 0) {
      val similarityThreshold = 0.97
      val occurenceThreshold = 50

      val movieID = args(0).toInt

      val similarMovies = movieSimilarities
        .filter(x => {
          val id1 = x._1._1
          val id2 = x._1._2
          val similarity = x._2._1
          val occurence = x._2._2
          (id1 == movieID || id2 == movieID) && similarity > similarityThreshold && occurence > occurenceThreshold
        })
        .map(x => (x._2, x._1)) // ((Similarity, Occurence), (MovieID1, MovieID2))
        .sortByKey(false)

      val count = similarMovies.count()
      println(f"Find $count similar movies for [${nameMapping(movieID)}]")

      val selected = if (count > 10) 10 else count.toInt
      println(f"Here is the top $selected ones")

      for ((item, index) <- similarMovies.take(selected).zipWithIndex) {
        val id1 = item._2._1
        val id2 = item._2._2
        val similarity = item._1._1
        val occurence = item._1._2

        val name = if (id1 == movieID) nameMapping(id2) else nameMapping(id1)

        println(f"# ${index + 1}: $name\tsimilarity: $similarity\toccurence: $occurence")
      }
    }
  }
}
