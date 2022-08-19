package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //coalesce默认情况下不会打乱分区重新组合
    val rdd = sc.makeRDD(List(1, 2, 3, 4,5,6),2)
    //shuffle为true会打乱分区重新组合
    //coalesce可以扩大分区，但是如果不进行suffer操作，则不起作用
    //val newRDD:RDD[Int] = rdd.coalesce(3,true)
    val newRDD:RDD[Int] = rdd.repartition(3)
    newRDD.saveAsTextFile("output")

    sc.stop()

  }
}
