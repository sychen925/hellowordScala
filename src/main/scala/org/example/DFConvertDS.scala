package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DFConvertDS {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    )) toDF("name", "age", "phone")

    df.as[Person].show()




//    val rdd = sc.makeRDD(List(Person("zhangsan",10),Person("lisi", 20),Person("wangwu", 30)))
//    rdd.toDS().show()
   // val df = rdd.toDF("name","age").show()


//    val ds = df.as[Person]
//
//    ds.toDF()





  }
}
