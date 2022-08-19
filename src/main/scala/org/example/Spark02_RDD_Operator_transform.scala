package org.example

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO算子 -mapPartitions : 可以以分区为单位进行数据转换操作
    //                          但是会将整个分区的数据加载到内存进行引用
    //                          处理完的数据是不会被释放掉----存在对象的引用
    //                          在内存较小，数据较大的场合下易出现内存溢出
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpRDD = rdd.mapPartitions(
      iter => {
        println("##########")
        iter.map(_ * 2)
      }
    )
    mpRDD.collect().foreach(println)
    sc.stop()


  }
}
