package sparta

import java.io._
import java.net._
import java.util
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


// TODO: pass Julia input and output types and Java serializer/deserializer class names
// TODO: In Julia, `convert(T, bytes)` should be used to decode input and `convert(Array{Uint8}, t::T)` to serialize output
// TODO: In Java, reflection should be used to instantiate serializer and deserializer
// TODO: for now we can concentrate on strings
class JuliaRDD(@transient parent: RDD[_], func: Array[Byte]) extends RDD[Array[Byte]](parent) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val inputIter = firstParent.iterator(split, context)

    println("julia func as string (nonsense!): " + new String(func))


//    // TODO: worker path should be different
//    val pb = new ProcessBuilder("julia", "-L", "src/worker.jl", "-e", "SpockWorker.worker()")
//    pb.redirectError(ProcessBuilder.Redirect.INHERIT)
//    val worker = pb.start()
//
//    // send input
//    val out = new DataOutputStream(new BufferedOutputStream(worker.getOutputStream()))
//    func.write(out)
//    out.writeInt(split.index.intValue())
//    while(it.hasNext) {
//      it.next().write(out)
//    }
//    out.writeInt(0)
//    out.close()
//
//    // read results
//    val in = new DataInputStream(new BufferedInputStream(worker.getInputStream()))
//    val results = new util.LinkedList[JuliaObject]()
//    while(true) {
//      val obj = JuliaObject.read(in);
//      if(obj == null) break
//      results.add(obj)
//    }
//
//    // finish up
//    if(worker.waitFor() != 0) {
//      throw new RuntimeException(String.format("Worker died with exitValue=%d", worker.exitValue()));
//    } else {
//      return results.iterator()
//    }

    val outputIter = new Iterator[Array[Byte]] {

      // TODO: read from stdin
      override def hasNext = false

      override def next = null

    }

    new InterruptibleIterator(context, outputIter)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent.partitions
  }

}
