package org.example

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, datediff, desc, expr, max, min, sum, trunc}

object LongestTimeSpan {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    val productData = readCVS(spark,"ProductData")
    val salesData = readCVS(spark,"SalesOrderData")
    val joinSalesAndProductTable = joinSalesAndProduct(salesData,productData,"ProductID")
    val longestTimeSpan = caculateTimeSpanTop50(joinSalesAndProductTable,"Orderdate","Modifieddate")
    saveCSV(longestTimeSpan,"./src/data/longestTimeSpan.csv")
    //test
  }

  def createSparkSession():SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    spark
  }

  def readCVS(sparkSession: SparkSession,csvName:String):DataFrame ={
    sparkSession.read.option("header", true).csv(s"./src/data/${csvName}.csv")
  }

  def joinSalesAndProduct(salesData:DataFrame,productData:DataFrame,joinCol:String) ={
    salesData.join(productData, Seq(joinCol))
  }

  def caculateTimeSpanTop50(joinSalesAndProduct:DataFrame,timeSpanCol1:String,timeSpanCol2:String) ={
    joinSalesAndProduct.select(col("SalesOrderDetailID"), col("Modifieddate"), col("Orderdate"), datediff(col(timeSpanCol1), col(timeSpanCol2)).alias("datediff"))
      .sort(col("datediff").desc).limit(50)
  }

  def saveCSV(csvDataframe:DataFrame,path:String): Unit ={
    csvDataframe.write.format("csv")
      .option("header",true)
      .option("delimiter","|")
      .csv(path)
  }

}
