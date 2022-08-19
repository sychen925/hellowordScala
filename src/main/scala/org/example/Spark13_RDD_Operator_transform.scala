package org.example

import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO算子 -双value
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    //交集 要求数据类型保持一致
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    //拉链
    val rdd4 = rdd1.zip(rdd2)
    println(rdd4.collect().mkString(","))

    sc.stop()

  }
}
