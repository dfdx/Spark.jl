package sparta

import java.io._
import java.net._
import java.util.{Collections, ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.spark.annotation.DeveloperApi

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.existentials

import com.google.common.base.Charsets.UTF_8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.util.control.NonFatal



class JuliaRDD(@transient parent: RDD[_]) extends RDD[Array[Byte]](parent) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    // use JuliaWebAPI for communication? is it possible to pass function closure using it?

    null
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent.partitions
  }
}
