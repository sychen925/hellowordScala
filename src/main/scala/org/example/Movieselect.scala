package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr, sum}
import org.apache.spark.sql.types.FloatType

object Movieselect {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    //    val movieData = spark.read.csv("./src/data/movies.csv")
    val movieData = spark.read.option("header", true).csv("./src/data/movies.csv")
    movieData.show()
    val repalceC6 = movieData.withColumn("Worldwide_Gross", expr("replace(Worldwide_Gross, '$', '')"))
    val df2= repalceC6.withColumn("Worldwide_Gross",col("Worldwide_Gross").cast(FloatType))
    //每个年份的电影票房排序
    val sortByYear = df2.groupBy("Year").agg(sum("Worldwide_Gross")).sort("sum(Worldwide_Gross)")
    //每个类型的电影的票房排序
    val sortByGenre = df2.groupBy("Genre").agg(sum("Worldwide_Gross")).sort("sum(Worldwide_Gross)")
    //筛选票房和评分都高于平均值的电影
    val avgScore = repalceC6.agg(avg("Audience_Score")).head().get(0)
    //    println(avgScore)
    val avgGross = repalceC6.agg(avg("Worldwide_Gross")).head().get(0)
    repalceC6.where(s"Audience_Score > ${avgScore} and Worldwide_Gross > ${avgGross}").show()
  }

}
