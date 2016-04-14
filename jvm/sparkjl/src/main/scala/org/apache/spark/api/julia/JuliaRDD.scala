package org.apache.spark.api.julia

import java.io._
import java.net._

import org.apache.commons.compress.utils.Charsets
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.language.existentials


class JuliaRDD(
    @transient parent: RDD[_],
    command: Array[Byte],
    inputType: Array[Byte]
) extends RDD[Array[Byte]](parent) {

  val preservePartitioning = true
  val reuseWorker = true

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitioning) firstParent.partitioner else None
  }


  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val worker: Socket = JuliaRDD.createWorker()
    // Start a thread to feed the process input from our parent's iterator
    val outputThread = new OutputThread(context, firstParent.iterator(split, context), worker, command, inputType, split)
    outputThread.start()
    // Return an iterator that read lines from the process's stdout
    val resultIterator = new InputIterator(context, worker, outputThread)
    new InterruptibleIterator(context, resultIterator)
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override def collect(): Array[Array[Byte]] = {
    super.collect()
  }


}

private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val JULIA_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
}

object JuliaRDD extends Logging {

  def fromJavaRDD[T](javaRdd: JavaRDD[T], command: Array[Byte], inputType: Array[Byte]): JuliaRDD =
    new JuliaRDD(JavaRDD.toRDD(javaRdd), command, inputType)

  def createWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1).map(_.toByte)))

      // Create and start the worker
      val pb = new ProcessBuilder(Seq("julia", "-e", "using Spark; using Iterators; Spark.launch_worker()"))
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


  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

    def write(obj: Any): Unit = {
      obj match {
        case arr: Array[Byte] =>
          dataOut.writeInt(arr.length)
          dataOut.write(arr)
        case str: String =>
          writeUTF(str, dataOut)
        case other =>
          throw new SparkException("Unexpected element type " + other.getClass)
      }
    }

    iter.foreach(write)
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(Charsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

}

