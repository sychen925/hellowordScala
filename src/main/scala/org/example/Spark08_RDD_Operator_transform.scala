package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO算子 -sample  数据倾斜的时候可以使用
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //sample算子需要传递三个参数
    //1、第一个参数：抽取数据后是否将数据返回 true：放回 false：丢弃
    //2、第二个参数：数据源中每条数据被抽取的概率
    //3、读三个参数：抽取数据时随机数的种子,如果不传递第三个参数，那么使用的是当前系统时间
    println(rdd.sample(false, 0.4).collect().mkString(","))
    sc.stop()

  }
}
