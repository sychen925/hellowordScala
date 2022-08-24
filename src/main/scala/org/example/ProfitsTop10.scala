package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, dayofweek, desc, expr, sum, trunc}

object ProfitsTop10 {
  def main(args: Array[String]): Unit = {
    val spark = sparkSession()
    val productData = readCSV(spark,"ProductData")
    val salesData = readCSV(spark,"SalesOrderData")
    val joinSalseAndProduct = joinTable(salesData,productData,"ProductID")
    val profits = profitsSort(joinSalseAndProduct)
    val totalProfitTop10 = selectProfitsTop10(profits)
    saveCSV(totalProfitTop10,"./src/data/totalProfitTop10.csv")
  }
  def sparkSession(): SparkSession ={
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark
  }

  def readCSV(spark: SparkSession,csvName:String):DataFrame ={
    spark.read.option("header", true).csv(s"./src/data/${csvName}.csv")
  }

  def joinTable(table1:DataFrame,table2:DataFrame,joinCol:String) ={
    table1.join(table2, Seq(joinCol))
  }

  def profitsSort(joinTable:DataFrame) ={
    joinTable.withColumn("profits", expr("Unitprice*Orderqty-StandardCost")).sort(col("profits").desc)
  }

  def selectProfitsTop10(profits:DataFrame) ={
    profits.limit(10).select(col("ProductID"),col("profits"))
  }

  def saveCSV(totalProfitTop10:DataFrame,path:String): Unit ={
    totalProfitTop10.write.format("csv")
      .option("header",true)
      .option("delimiter","|")
      .csv(path)
  }

}
