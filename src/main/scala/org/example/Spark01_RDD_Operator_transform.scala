package org.example

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO算子 -map
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD1 = rdd.map(
      num => {
        println("<<<<<<<<<<"+num)
        num
      }
    )
    val mapRDD2 = mapRDD1.map(
      num => {
        println("##########"+num)
        num
      }
    )

    mapRDD2.collect()
    sc.stop()

  }

}
