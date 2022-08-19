package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO算子 -flatMap
    val rdd = sc.makeRDD(List(
      List(1, 2), 3,List(4, 5)
    ))

    val value = rdd.flatMap(
      data => {
        data match {
          case list:List[_] => list
          case dat => List(dat)
        }
      }
    )

    value.collect().foreach(println)
    sc.stop()

  }
}
