package org.apache.spark.api.julia

import org.apache.spark.SparkContext


object Main {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "hello")
    val rdd = sc.textFile("file:///var/log/syslog")
    rdd.collect()
    val jlRdd = new JuliaRDD(rdd, Array(1, 2, 3, 4, 5).map(_.toByte))
    jlRdd.collect()
    //println("ok, julia")
  }

//  def main(args: Array[String]): Unit = {
//    val juliaWorkerFactory = new JuliaWorkerFactory(Map())
//    juliaWorkerFactory.create()
//  }

}
