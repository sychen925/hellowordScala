package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, sum, trunc}

object ProductSelect {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    val productData = readCSV(spark,"ProductData.csv")
    val salesData = readCSV(spark,"SalesOrderData.csv")
    val customerAddressData = readCSV(spark,"CustomerAddress-data.csv")
    val caculatedProfitsDataframe = caculateProfits(productData, salesData, customerAddressData)
    val orderDateMonth = addMonthCol(caculatedProfitsDataframe)
    val totalDue = sumTotalDueAndProfitsByIdAndMonth(orderDateMonth)
    saveToCSV(totalDue,"./src/data/salesProfitDiffCities.csv")
    //test

  }
  def createSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark
  }

  def readCSV(sparkSession: SparkSession,csvName:String): DataFrame ={
    sparkSession.read.option("header", true).csv(s"./src/data/${csvName}")
  }

  def caculateProfits(productData:DataFrame,salesData:DataFrame,customerData:DataFrame) ={
    val joinTableProdAndSales = salesData.join(productData, salesData("ProductID") === productData("ProductID"))
    val joinThreeTable = joinTableProdAndSales.join(customerData, customerData("CustomerID") === salesData("CustomerID"))
    val joinThreeTableProfits = joinThreeTable.withColumn("profits", expr("Unitprice*Orderqty-StandardCost"))
    joinThreeTableProfits
  }

  def addMonthCol(joinThreeTableProfits:DataFrame) ={
    val orderDateMonth = joinThreeTableProfits.withColumn("OrderDateMonth", trunc(col("OrderDate"), "Month"))
    orderDateMonth
  }

  def sumTotalDueAndProfitsByIdAndMonth(orderDateMonth:DataFrame)={
    orderDateMonth.groupBy("AddressID", "OrderDateMonth").agg(sum("TotalDue"), sum("profits")).sort("sum(TotalDue)")
  }

  def saveToCSV(totalDue:DataFrame,path:String): Unit ={
    totalDue.write.format("csv")
      .option("header", true)
      .option("delimiter", "|")
      .csv(path)
  }
}
