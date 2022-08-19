package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object ProductAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val productData = spark.read.option("header", true).csv("./src/data/ProductData.csv")
    val salesData = spark.read.option("header", true).csv("./src/data/SalesOrderData.csv")
    val joinTable = salesData.join(productData, salesData("ProductID") === productData("ProductID")).show()



  }

}
