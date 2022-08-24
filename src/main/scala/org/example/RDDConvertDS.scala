package org.example

import org.apache.spark.{SparkConf, SparkContext}

object RDDConvertDS {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    val rdd = sc.makeRDD(List(("zhangsan",10),("lisi", 20),("wangwu", 30)))
    val df = rdd.toDF("name","age").show()


    //val ds = df.as[Person]





  }
}
