package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO算子 -key-value
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD:RDD[(Int,Int)] = rdd.map((_, 1))
    //RDD=>PairRDDFuntion
    //隐式转换（二次编译）
    //partitionBy根据指定的分区规则对数据进行重分区
    mapRDD.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")

    sc.stop()

  }
}
