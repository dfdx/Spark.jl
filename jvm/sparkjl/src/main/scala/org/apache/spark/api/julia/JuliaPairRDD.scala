package org.apache.spark.api.julia

import java.io._
import java.net._

import org.apache.commons.compress.utils.Charsets
import org.apache.spark._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.language.existentials


class JuliaPairRDD(
    @transient parent: RDD[_],
    command: Array[Byte]
) extends RDD[(Any, Any)](parent) {

  val preservePartitioning = true
  val reuseWorker = true

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitioning) firstParent.partitioner else None
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(Any, Any) ] = {
    val worker: Socket = JuliaPairRDD.createWorker()
    // Start a thread to feed the process input from our parent's iterator
    val outputThread = new OutputThread(context, firstParent.iterator(split, context), worker, command, split)
    outputThread.start()
    // Return an iterator that read lines from the process's stdout
    val resultIterator = new InputPairIterator(context, worker, outputThread)
    new InterruptibleIterator(context, resultIterator)
  }

  override def collect(): Array[(Any, Any)] = {
    super.collect()
  }

  def asJavaPairRDD(): JavaPairRDD[Any, Any] = {
    JavaPairRDD.fromRDD(this)
  }
}

object JuliaPairRDD extends Logging {

  def fromRDD[T](rdd: RDD[T], command: Array[Byte]): JuliaPairRDD =
    new JuliaPairRDD(rdd, command)

  def createWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1).map(_.toByte)))

      // Create and start the worker
      val pb = new ProcessBuilder(Seq("julia", "-e", "using Spark; using Iterators; Spark.launch_worker()"))
      pb.directory(new File(SparkFiles.getRootDirectory()))
      // val workerEnv = pb.environment()
      // workerEnv.putAll(envVars)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      StreamUtils.redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        val socket = serverSocket.accept()
        // workers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("Julia worker did not connect back in time", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

}

