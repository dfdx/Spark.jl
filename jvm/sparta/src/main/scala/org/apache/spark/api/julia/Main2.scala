package org.apache.spark.api.julia

import org.apache.spark.api.java.JavaSparkContext


object Main2 {

  def main(args: Array[String]): Unit = {
    val sc = new JavaSparkContext("local", "hello")
    val rdd = sc.parallelize(List(
      Array(1, 2, 3).map(_.toByte),
      Array(4, 5, 6).map(_.toByte),
      Array(5, 6, 7).map(_.toByte)))
    val cmd = Array(19, 0, 2, 8, 105, 100, 101, 110, 116, 105, 116, 121).map(_.toByte)
    val inputType = Array(1, 2, 3, 4).map(_.toByte)
    val jlRdd = JuliaRDD.fromJavaRDD(rdd, cmd, inputType)
    jlRdd.collect()
  }


}
