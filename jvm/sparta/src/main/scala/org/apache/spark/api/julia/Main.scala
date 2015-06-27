package org.apache.spark.api.julia

import org.apache.spark.SparkContext


object Main {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "hello")
    val rdd = sc.textFile("file:///var/log/syslog")
    //new JuliaRDD(null, null)
    //println("ok, julia")
  }

}
