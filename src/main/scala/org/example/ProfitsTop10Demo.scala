package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, sum, trunc}

object ProfitsTop10Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    //什么颜色的product的销售额最高
    val productData = spark.read.option("header", true).csv("./src/data/ProductData.csv")
    val salesData = spark.read.option("header", true).csv("./src/data/SalesOrderData.csv")
    val joinTable = salesData.join(productData, salesData("ProductID") === productData("ProductID"))
    //每月不同城市客户的销售额，利润额
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
//    total_due.show()
    val profits = joinTable.withColumn("profits", expr("Unitprice*Orderqty-StandardCost"))
    //join sales product address三个表
    val joinTable3 = profits.join(joinTable,profits("CustomerID") === joinTable("CustomerID"))
    val total_profits = joinTable3.groupBy("AddressID", "OrderDate").agg(sum("profits")).sort(col("sum(profits)").desc)
    val totalProfitTop10 = total_profits.limit(10)


    totalProfitTop10.write.format("csv")
      .option("header",true)
      .option("delimiter","|")
      .csv("./src/data/totalProfitTop10.csv")










  }

}
