package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO算子 -key-value
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    //将数据源中的数据，相同的key的数据分在一个组中，形成一个对偶元祖
    //              元组中的第一个元素就是key，
    //              元组中的第二个元素就是相同的key的value的集合
    val groupByKeyRDD:RDD[(String,Iterable[Int])] = rdd.groupByKey()
    groupByKeyRDD.collect().foreach(println)

    val groupRDD = rdd.groupBy(_._1)
    groupRDD.collect().foreach(println)

    sc.stop()

  }
}
