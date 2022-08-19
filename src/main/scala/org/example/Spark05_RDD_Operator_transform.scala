package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO算子 -glom
    val rdd :RDD[Int]= sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD :RDD[Array[Int]]= rdd.glom()

    //glomRDD.collect().foreach(data=>println(data.mkString(",")))

    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )


    println(maxRDD.collect().sum)
    sc.stop()

  }
}
