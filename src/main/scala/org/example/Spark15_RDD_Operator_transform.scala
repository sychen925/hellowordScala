package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO算子 -key-value
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作都是两两聚合，spark是基于scala开发的，所以也是两两聚合
    //reduceByKey中如果key的数据只有一个，是不会参加运算的
    val reduceRDD:RDD[(String,Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x},y=${y}")
      x + y
    })

    reduceRDD.collect().foreach(println)
    sc.stop()

  }
}
