package org.apache.spark.api.julia

import org.apache.spark.SparkContext


object Main {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "hello")
    val rdd = sc.parallelize(List(
      Array(1, 2, 3).map(_.toByte),
      Array(4, 5, 6).map(_.toByte),
      Array(5, 6, 7).map(_.toByte)))
    val jlRdd = new JuliaRDD(rdd, Array(1, 2, 3, 4, 5).map(_.toByte))
    jlRdd.collect()
  }


}
