package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO算子 -key-value
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("b", 4),("b",5),("a",6)
    ),2)

    //aggregateByKey
    //获取相同key的数据的平均值=》（a，3）(b,4)
    val newRDD:RDD[(String,(Int,Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val result = newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    result.collect().foreach(println)
    sc.stop()

  }
}
