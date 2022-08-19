package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO算子 -key-value
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    //aggregateByKey存在函数柯里化，有两个参数列表
    //第一个参数列表：表示分区内计算规则,需要传递一个参数，主要用于碰见第一个key的时候，和value进行分区计算
    //第二个参数列表：表示分区间计算规则
    rdd.aggregateByKey(5)(
      (x,y) => math.max(x,y),
      _+_
    ).collect().foreach(println)

    //如果聚合计算时，分区内和分区见计算规则相同，spark提供了简化的计算方法
    //foldByKey
    //rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop()

  }
}
