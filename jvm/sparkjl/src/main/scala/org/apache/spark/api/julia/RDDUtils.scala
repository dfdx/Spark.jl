package org.apache.spark.api.julia

import org.apache.spark.internal.Logging
import org.apache.spark.api.java.{JavaRDD, JavaPairRDD}

object RDDUtils extends Logging {

  /**
   * Get number of partitions in the RDD
   */
  def getNumPartitions(jrdd: JavaRDD[Any]): Int = jrdd.rdd.partitions.length
  def getNumPartitions(jrdd: JavaPairRDD[Any,Any]): Int = jrdd.rdd.partitions.length
}
