package org.example
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
object movieSelect {
  def main(args: Array[String]): Unit = {

    //val df = spark.read.csv("src/data/movies.csv")
    val session = SparkSession.builder().appName("movies.csv").master("local").getOrCreate()
    val movieData = session.read
      .csv("/Users/shiyu.chenthoughtworks.com/github/scalatest/scalatest01/src/data")
    movieData.show
    //根据年份票房排序
    val rows = movieData.collect().length
    println("==============="+ rows)

    //val minYear = movieData.select("_c6").collect.array(32).toString().substring(2,minYearLength-2).toFloat
    val a=1
    var movieAccount2007 = 0.0
    for(i <- 1  to rows-1){
      if (movieData.select("_c7").collect()(i).toString()== "[2007]"){
        //println("qqq")
        val minYearLength = movieData.select("_c6").collect.array(i-1).toString().length
        movieAccount2007=movieAccount2007 + movieData.select("_c6").collect.array(i-1).toString().substring(2,minYearLength-2).toFloat
      }
    }
    println("================================"+movieAccount2007)
    //val minYear = movieData.collect()(6)


  }
}
